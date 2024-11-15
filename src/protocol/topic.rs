use super::KafkaError;

#[allow(dead_code)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TopicOp {
    Unknown,
    Any,
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
    CreateTokens,
    DescribeTokens,
}

impl TopicOp {
    pub fn default_ops() -> Vec<Self> {
        vec![
            Self::Read,
            Self::Write,
            Self::Create,
            Self::Delete,
            Self::Alter,
            Self::Describe,
            Self::DescribeConfigs,
            Self::AlterConfigs,
        ]
    }
}

fn authorized_operations(ops: Vec<TopicOp>) -> i32 {
    let mut auth = 0;
    for op in ops {
        match op {
            TopicOp::Unknown => auth |= 1,
            TopicOp::Any => auth |= 2,
            TopicOp::All => auth |= 4,
            TopicOp::Read => auth |= 8,
            TopicOp::Write => auth |= 16,
            TopicOp::Create => auth |= 32,
            TopicOp::Delete => auth |= 64,
            TopicOp::Alter => auth |= 128,
            TopicOp::Describe => auth |= 256,
            TopicOp::ClusterAction => auth |= 512,
            TopicOp::DescribeConfigs => auth |= 1024,
            TopicOp::AlterConfigs => auth |= 2048,
            TopicOp::IdempotentWrite => auth |= 4096,
            TopicOp::CreateTokens => auth |= 8192,
            TopicOp::DescribeTokens => auth |= 16384,
        }
    }

    auth
}

#[derive(Debug)]
pub struct Partition;

#[derive(Debug)]
pub struct Topic {
    pub error: KafkaError,
    pub id: [u8; 16],
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<Partition>,
    pub authorized_operations: i32,
}

impl Topic {
    pub fn unknown(name: &str) -> Self {
        let ops = TopicOp::default_ops();
        let authorized_operations = authorized_operations(ops);

        Self {
            error: KafkaError::UnknownTopicOrPartition,
            name: name.to_owned(),
            id: [0; 16],
            internal: false,
            partitions: vec![],
            authorized_operations,
        }
    }
}
