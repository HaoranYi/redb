use std::any::type_name;
use std::cmp::Ordering;
use std::fmt::Debug;

use bincode::{deserialize, serialize};
use redb::{Database, Error, Key, Range, ReadableTableMetadata, TableDefinition, TypeName, Value};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]

struct Pubkey([u8; 32]);

impl From<[u8; 32]> for Pubkey {
    fn from(from: [u8; 32]) -> Self {
        Self(from)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SomeKey {
    foo: Pubkey,
    bar: i32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct SomeValue {
    foo: [f64; 3],
    bar: bool,
}

const TABLE: TableDefinition<([u8; 32], i32), Bincode<SomeValue>> = TableDefinition::new("my_data");
//const TABLE: TableDefinition<(Pubkey, i32), Bincode<SomeValue>> = TableDefinition::new("my_data");

fn main() -> Result<(), Error> {
    // let some_key = ([1_u8; 32], 42);
    let some_key = SomeKey {
        foo: Pubkey::from([1_u8; 32]),
        bar: 42,
    };
    let some_key = (some_key.foo.0, some_key.bar);

    let some_key2 = ([1_u8; 32], 43);

    let some_value = SomeValue {
        foo: [1., 2., 3.],
        bar: true,
    };
    let lower = ([1_u8; 32], 0);
    let upper = ([1_u8; 32], 50);

    let db = Database::create("slot_keys.redb")?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;

        table.insert(&some_key, &some_value).unwrap();
        table.insert(&some_key2, &some_value).unwrap();
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;

    println!("table len: {}", table.len()?);

    let mut iter: Range<([u8; 32], i32), Bincode<SomeValue>> = table.range(lower..upper).unwrap();
    let (key, value) = iter.next().unwrap().unwrap();
    println!("{:?} {:?}", key.value(), value.value());
    assert_eq!(value.value(), some_value);

    let (key, value) = iter.next().unwrap().unwrap();
    println!("{:?} {:?}", key.value(), value.value());
    assert_eq!(value.value(), some_value);

    assert!(iter.next().is_none());

    Ok(())
}

/// Wrapper type to handle keys and values using bincode serialization
#[derive(Debug)]
pub struct Bincode<T>(pub T);

impl<T> Value for Bincode<T>
where
    T: Debug + Serialize + for<'a> Deserialize<'a>,
{
    type SelfType<'a> = T
    where
        Self: 'a;

    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        deserialize(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        serialize(value).unwrap()
    }

    fn type_name() -> TypeName {
        TypeName::new(&format!("Bincode<{}>", type_name::<T>()))
    }
}

impl<T> Key for Bincode<T>
where
    T: Debug + Serialize + DeserializeOwned + Ord,
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}
