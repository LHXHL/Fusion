use std::io::{Error, ErrorKind};

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc};

use crate::protocol::{
    frame::{Frame, MessageType},
    message::{Message, StreamCloseMessage, StreamDataMessage},
};

pub fn build_stream_data_frame(
    local_agent_id: &str,
    remote_agent_id: &str,
    stream_id: u32,
    bytes: &[u8],
) -> Frame {
    Frame::new(
        MessageType::StreamData,
        Some(local_agent_id.to_string()),
        Some(remote_agent_id.to_string()),
        Message::StreamData(StreamDataMessage::from_bytes(bytes)),
    )
    .with_stream_id(stream_id)
}

pub fn build_stream_close_frame(
    local_agent_id: &str,
    remote_agent_id: &str,
    stream_id: u32,
) -> Frame {
    Frame::new(
        MessageType::StreamClose,
        Some(local_agent_id.to_string()),
        Some(remote_agent_id.to_string()),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(stream_id)
}

pub async fn write_next_stream_data_to_client(
    rx: &mut mpsc::Receiver<Frame>,
    client: &mut TcpStream,
    stream_id: u32,
    context: &str,
) -> Result<bool, Error> {
    let response = rx
        .recv()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "stream receiver closed"))?;
    if response.header.stream_id != Some(stream_id) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("unexpected stream_id on {context} response"),
        ));
    }
    match response.message {
        Message::StreamData(data) => {
            let bytes = data.to_bytes()?;
            client.write_all(&bytes).await?;
            client.flush().await?;
            Ok(true)
        }
        Message::StreamClose(_) => Ok(false),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected StreamData response, got {:?}", other),
        )),
    }
}

pub async fn expect_stream_close_ack(
    rx: &mut mpsc::Receiver<Frame>,
    stream_id: u32,
) -> Result<(), Error> {
    let close_ack = rx
        .recv()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "close ack receiver closed"))?;
    if close_ack.header.stream_id != Some(stream_id) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "unexpected stream_id on close ack",
        ));
    }
    match close_ack.message {
        Message::StreamClose(_) => Ok(()),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected StreamClose ack, got {:?}", other),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_stream_close_frame, build_stream_data_frame, expect_stream_close_ack,
        write_next_stream_data_to_client,
    };
    use crate::protocol::{
        frame::MessageType,
        message::{Message, StreamCloseMessage},
    };
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };

    async fn tcp_socket_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        (client.await.unwrap(), server)
    }

    #[test]
    fn build_stream_frames_preserve_ids() {
        let data = build_stream_data_frame("local-a", "remote-b", 7, b"abc");
        assert_eq!(data.header.msg_type, MessageType::StreamData);
        assert_eq!(data.header.stream_id, Some(7));

        let close = build_stream_close_frame("local-a", "remote-b", 8);
        assert_eq!(close.header.msg_type, MessageType::StreamClose);
        assert_eq!(close.header.stream_id, Some(8));
    }

    #[tokio::test]
    async fn write_next_stream_data_to_client_writes_payload() {
        let (mut client, mut server) = tcp_socket_pair().await;
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(build_stream_data_frame(
            "local-a",
            "remote-b",
            9,
            b"bridge-ok",
        ))
        .await
        .unwrap();

        let wrote = write_next_stream_data_to_client(&mut rx, &mut server, 9, "bridge")
            .await
            .unwrap();
        assert!(wrote);

        let mut buf = [0_u8; 16];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"bridge-ok");
    }

    #[tokio::test]
    async fn expect_stream_close_ack_accepts_close_message() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(
            crate::protocol::frame::Frame::new(
                MessageType::StreamClose,
                Some("local-a".into()),
                Some("remote-b".into()),
                Message::StreamClose(StreamCloseMessage {
                    reason: Some("ok".into()),
                }),
            )
            .with_stream_id(11),
        )
        .await
        .unwrap();

        expect_stream_close_ack(&mut rx, 11).await.unwrap();
    }
}
