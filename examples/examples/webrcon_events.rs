//! WebRCON JSON protocol correlation example demonstrating runtime-independent serialization
//! and classification of command responses versus unsolicited console/chat broadcasts.

use rconet::{PacketId, WebRconEvent, WebRconMessageKind, WebRconRequest};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request_id = PacketId::from_raw(101);
    let outbound = WebRconRequest::new(request_id, "serverinfo");
    println!("Outbound WebRCON JSON: {}", outbound.to_json()?);

    let incoming_frames = [
        r#"{"Identifier":0,"Message":"[Chat] PlayerOne: gg","Type":"Chat","Stacktrace":""}"#,
        r#"{"Identifier":101,"Message":"{\"Hostname\":\"Rust Island\",\"Players\":42}","Type":"Generic","Stacktrace":""}"#,
        r#"{"Identifier":-1,"Message":"Saved 28,410 entities","Type":"Log","Stacktrace":""}"#,
    ];

    for raw in incoming_frames {
        let event = WebRconEvent::from_json(raw, 65_536)?;
        if event.is_unsolicited_broadcast() {
            println!("Live broadcast ({:?}): {}", event.kind, event.message);
        } else if event.packet_id() == request_id {
            println!(
                "Correlated reply for id {} ({:?}): {}",
                request_id.as_i32(),
                WebRconMessageKind::Generic,
                event.message
            );
        }
    }

    Ok(())
}
