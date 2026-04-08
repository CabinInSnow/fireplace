use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::OnceLock;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);
static NODE_ID: OnceLock<u64> = OnceLock::new();

fn get_node_id() -> u64 {
    *NODE_ID.get_or_init(|| {
        // 환경변수로 명시적 주입 (K8s/Docker 환경 권장)
        if let Ok(id_str) = std::env::var("SNOWFLAKE_NODE_ID") {
            if let Ok(id) = id_str.parse::<u64>() {
                return id & 0x3FF; // 10 bits
            }
        }

        // Fallback: Docker나 K8s 환경에서는 HOSTNAME 환경 변수에 Pod ID가 주입됩니다.
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| {
                format!("node_{}_{}", std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())
            });

        // Hostname 문자열을 이용해 간단한 Hash 생성 (10 bit 커버리지 확보)
        let mut hash_val: u64 = 0;
        for (i, byte) in hostname.bytes().enumerate() {
            hash_val = hash_val.wrapping_add((byte as u64).wrapping_mul((i as u64) + 1));
        }
        
        hash_val & 0x3FF // 10 bits limit
    })
}

/// 엔진에서 제공하는 기본적인 분산/고유 ID (Snowflake 방식 유사) 생성기입니다.
/// 실 서비스 적용 시 Worker ID 분배 등 환경 변수를 통한 커스텀이 권장됩니다.
pub fn generate_snowflake() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;

    // 41 bits Time | 10 bits Node | 12 bits Sequence (기본적인 Twitter Snowflake 구조 참조)
    let node_id = get_node_id();
    let seq = SEQUENCE.fetch_add(1, Ordering::Relaxed) & 0xFFF; // 12 bits

    // 기준 시간 (예: 2024-01-01) - 필요한 경우 조정 가능
    let epoch = 1704067200000u64;
    let timestamp = now.saturating_sub(epoch);

    (timestamp << 22) | ((node_id & 0x3FF) << 12) | seq
}

/// 엔진 로깅을 초기화하는 헬퍼 함수입니다. 게임 서버 진입점에서 사용할 수 있습니다.
pub fn init_logger(level: tracing::Level) {
    tracing_subscriber::fmt()
        .with_max_level(level)
        .init();
}

const BASE62_ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Snowflake ID를 12자리 고정 길이의 Base62 문자열(Display ID)로 변환합니다.
pub fn snowflake_to_display_id(uid: u64) -> String {
    let mut n = uid;
    let mut res = Vec::new();
    
    if n == 0 {
        res.push(b'0');
    } else {
        while n > 0 {
            res.push(BASE62_ALPHABET[(n % 62) as usize]);
            n /= 62;
        }
    }
    
    // 12자리 고정을 위해 앞에 '0' 패딩 추가
    while res.len() < 12 {
        res.push(b'0');
    }
    
    res.reverse();
    String::from_utf8(res).unwrap()
}

/// Snowflake ID를 19자리 고정 길이의 10진수 문자열로 변환합니다.
pub fn snowflake_to_string(uid: u64) -> String {
    format!("{:019}", uid)
}
