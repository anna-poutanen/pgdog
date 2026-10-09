use crate::setup::{admin_sqlx, connections_tokio};
use futures_util::FutureExt;
use sqlx::Executor;
use std::panic::{AssertUnwindSafe, resume_unwind};
use tokio_postgres::{SimpleQueryMessage, types::Type};

#[tokio::test]
async fn explain_unique_id_preserves_client_parameters() -> Result<(), Box<dyn std::error::Error>> {
    let admin = admin_sqlx().await;
    admin.execute("SET rewrite_enabled TO true").await?;
    let result = AssertUnwindSafe(async {
        for client in connections_tokio().await {
            for options in ["VERBOSE", "ANALYZE, VERBOSE"] {
                let sql = format!(
                    "EXPLAIN ({options}) {}",
                    "SELECT $2::text, pgdog.unique_id(), $1::bigint, pgdog.unique_id()"
                );
                let statement = client.prepare(&sql).await?;
                assert_eq!(statement.params(), &[Type::INT8, Type::TEXT]);
                for (value, label) in [(42_i64, "first_bind"), (73_i64, "second_bind")] {
                    let rows = client.query(&statement, &[&value, &label]).await?;
                    let plan = rows
                        .iter()
                        .map(|row| row.get::<_, String>(0))
                        .collect::<Vec<_>>()
                        .join("\n");
                    assert!(
                        plan.contains(&format!("'{label}'::text")),
                        "client text was lost: {plan}"
                    );
                    assert!(
                        plan.contains(&format!("'{value}'::bigint")),
                        "client bigint was lost: {plan}"
                    );
                    assert!(
                        !plan.contains("pgdog.unique_id()"),
                        "function was not rewritten: {plan}"
                    );
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .catch_unwind()
    .await;
    // Await restoration even when an assertion panics, before the runtime exits.
    let cleanup = admin.execute("RELOAD").await;
    match result {
        Ok(result) => {
            cleanup?;
            result
        }
        Err(panic) => {
            if let Err(error) = cleanup {
                eprintln!("failed to restore EXPLAIN test configuration: {error}");
            }
            resume_unwind(panic)
        }
    }
}

#[tokio::test]
async fn explain_routing_annotations_surface() -> Result<(), Box<dyn std::error::Error>> {
    let mut clients = connections_tokio().await;
    let sharded = clients.swap_remove(1);

    for shard in [0, 1] {
        let drop = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS explain_avg_test",
            shard
        );
        sharded.simple_query(drop.as_str()).await.ok();
    }

    for shard in [0, 1] {
        let create = format!(
            "/* pgdog_shard: {} */ CREATE TABLE explain_avg_test(price DOUBLE PRECISION, customer_id BIGINT)",
            shard
        );
        sharded.simple_query(create.as_str()).await?;
    }

    admin_sqlx().await.execute("RELOAD").await?;

    sharded
        .simple_query(
            "/* pgdog_shard: 0 */ INSERT INTO explain_avg_test(price) VALUES (10.0), (14.0)",
        )
        .await?;
    sharded
        .simple_query(
            "/* pgdog_shard: 1 */ INSERT INTO explain_avg_test(price) VALUES (18.0), (22.0)",
        )
        .await?;

    let rows = sharded
        .simple_query("EXPLAIN SELECT AVG(price) FROM explain_avg_test")
        .await?;

    let mut plan_lines = vec![];
    for message in rows {
        if let SimpleQueryMessage::Row(row) = message {
            plan_lines.push(row.get(0).unwrap_or_default().to_string());
        }
    }

    assert!(
        plan_lines.iter().any(|line| line.contains("Aggregate")),
        "missing Aggregate node in EXPLAIN output: {:?}",
        plan_lines
    );
    let routing_header = plan_lines
        .iter()
        .find(|line| line.contains("PgDog Routing:"));
    assert!(
        routing_header.is_some(),
        "missing PgDog Routing header in EXPLAIN output: {:?}",
        plan_lines
    );
    let summary_line = plan_lines
        .iter()
        .find(|line| line.contains("Summary:"))
        .cloned();
    assert!(
        summary_line
            .as_ref()
            .map(|line| line.contains("Summary: shard=all"))
            .unwrap_or(false),
        "unexpected summary line: {:?} (all lines: {:?})",
        summary_line,
        plan_lines
    );
    let broadcast_line = plan_lines
        .iter()
        .find(|line| line.contains("no sharding key matched"))
        .cloned();
    assert!(
        broadcast_line.is_some(),
        "missing broadcast note in EXPLAIN output: {:?}",
        plan_lines
    );

    for shard in [0, 1] {
        let drop = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS explain_avg_test",
            shard
        );
        sharded.simple_query(drop.as_str()).await.ok();
    }

    Ok(())
}
