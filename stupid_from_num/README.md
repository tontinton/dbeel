A derive macro that adds a `impl From<u8>` to any enum.

If the number of items is bigger than `u8::MAX`, it will use `u16`, then `u32` if bigger than `u16::MAX`.

Example:

```rust
use stupid_from_num::FromNum;

#[derive(FromNum)]
enum Example {
    Zero,
    One,
    Two,
}

assert_eq!(Example::Zero.into(), 0);
assert_eq!(Example::One.into(), 1);
assert_eq!(Example::Two.into(), 2);
```
