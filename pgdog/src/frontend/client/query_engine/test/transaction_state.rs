use crate::{
    config::load_test,
    expect_message,
    net::{
        BindComplete, CommandComplete, DataRow, ErrorResponse, Message, Parameters, ParseComplete,
        ReadyForQuery, bind::Parameter,
    },
};

use super::prelude::*;

/// Test that transaction state is tracked correctly through
/// BEGIN, extended protocol operations, and COMMIT.
#[tokio::test]
async fn test_transaction_state() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    // BEGIN
    client.send(Query::new("BEGIN")).await;
    client.try_process().await.unwrap();

    assert!(client.client().transaction.is_some());
    assert!(client.engine.router().command().route().is_write());

    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'T');

    // Parse + Describe + Sync (in transaction)
    client.send(Parse::named("test", "SELECT $1")).await;
    client.send(Describe::new_statement("test")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(client.client().transaction.is_some());
    assert!(client.engine.router().command().route().is_write());

    expect_message!(client.read().await, ParseComplete);
    client.read_until('Z').await.unwrap();

    // Bind + Execute + Sync (in transaction)
    client
        .send(Bind::new_params(
            "test",
            &[Parameter {
                len: 1,
                data: "1".as_bytes().into(),
            }],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    assert!(client.client().transaction.is_some());
    assert!(client.engine.router().command().route().is_write());

    expect_message!(client.read().await, BindComplete);
    expect_message!(client.read().await, DataRow);
    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'T');

    // COMMIT
    client.send(Query::new("COMMIT")).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, CommandComplete);
    let rfq = expect_message!(client.read().await, ReadyForQuery);
    assert_eq!(rfq.status, 'I');

    assert!(client.client().transaction.is_none());
}

async fn read_until_ready(client: &mut TestClient) -> Vec<Message> {
    let mut messages = Vec::new();
    loop {
        let message = client.read().await;
        let done = message.code() == 'Z';
        messages.push(message);
        if done {
            return messages;
        }
    }
}

async fn transaction_alias_recovers_failed_transaction(sql: &str, messages: &[ProtocolMessage]) {
    // A single primary makes the default Auto parser use the regex gate.
    load_test();
    let mut client = TestClient::new(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    let begun = client.read_until('Z').await.expect("begin transaction");
    assert_eq!(
        expect_message!(begun.last().expect("begin response").clone(), ReadyForQuery).status,
        'T'
    );

    client.send_simple(Query::new("SELECT 1 / 0")).await;
    let failed = read_until_ready(&mut client).await;
    let error = failed
        .iter()
        .find(|message| message.code() == 'E')
        .expect("division by zero must fail");
    assert_eq!(expect_message!(error.clone(), ErrorResponse).code, "22012");
    assert_eq!(
        expect_message!(
            failed.last().expect("failed response").clone(),
            ReadyForQuery
        )
        .status,
        'E'
    );

    for message in messages {
        client.send(message.clone()).await;
    }
    client.try_process().await.expect("process transaction end");
    let ended = read_until_ready(&mut client).await;
    assert!(
        ended.iter().all(|message| message.code() != 'E'),
        "{sql} must recover the failed transaction: {ended:?}"
    );
    let complete = ended
        .iter()
        .find(|message| message.code() == 'C')
        .expect("transaction end must complete");
    assert_eq!(
        expect_message!(complete.clone(), CommandComplete).command(),
        "ROLLBACK"
    );
    assert_eq!(
        expect_message!(
            ended.last().expect("transaction end response").clone(),
            ReadyForQuery
        )
        .status,
        'I'
    );
    assert!(client.client().transaction.is_none());

    client.send_simple(Query::new("SELECT 1")).await;
    let healthy = client.read_until('Z').await.expect("query after rollback");
    let row = healthy
        .iter()
        .find(|message| message.code() == 'D')
        .expect("healthy query must return a row");
    assert_eq!(
        expect_message!(row.clone(), DataRow).get_int(0, true),
        Some(1)
    );
}

#[tokio::test]
async fn transaction_aliases_recover_failed_transaction_simple() {
    for sql in ["ABORT", "END"] {
        transaction_alias_recovers_failed_transaction(sql, &[Query::new(sql).into()]).await;
    }
}

#[tokio::test]
async fn transaction_aliases_recover_failed_transaction_extended() {
    for sql in ["ABORT", "END"] {
        transaction_alias_recovers_failed_transaction(
            sql,
            &[
                Parse::new_anonymous(sql).into(),
                Bind::new_statement("").into(),
                Execute::new().into(),
                Sync.into(),
            ],
        )
        .await;
    }
}
