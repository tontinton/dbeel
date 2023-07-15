use std::net::ToSocketAddrs;

use dbeel::error::{Error, Result};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rmpv::{decode::read_value, encode::write_value, Utf8String, Value};

async fn send_request<A: ToSocketAddrs>(
    request: Value,
    address: A,
) -> Result<Value> {
    let mut data_encoded: Vec<u8> = Vec::new();
    write_value(&mut data_encoded, &request).unwrap();

    let mut stream = TcpStream::connect(address).await.unwrap();

    let size_buffer = (data_encoded.len() as u16).to_le_bytes();
    stream.write_all(&size_buffer).await?;
    stream.write_all(&data_encoded).await?;

    let mut response_buffer = Vec::new();
    stream.read_to_end(&mut response_buffer).await?;

    Ok(read_value(&mut &response_buffer[..])?)
}

pub async fn create_collection<S, A>(name: S, address: A) -> Result<()>
where
    S: Into<Utf8String>,
    A: ToSocketAddrs,
{
    let response = send_request(
        Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("create_collection".into()),
            ),
            (Value::String("name".into()), Value::String(name.into())),
        ]),
        address,
    )
    .await?;

    if response != Value::String("OK".into()) {
        return Err(Error::ResponseError("not OK".to_string()));
    }

    Ok(())
}
