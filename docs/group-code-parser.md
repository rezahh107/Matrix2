# Group Code Parser Spec
Grammar:
- Accepts tokens separated by comma `,`, Persian comma `،`, or whitespace.
- A range token `a:b` expands to all integers from a to b inclusive.
- Single integers allowed. Persian/English digits allowed.

Examples:
- "1,3,5,7:9"  -> [1,3,5,7,8,9]
- "۲ ، ۴ ، ۶:۸" -> [2,4,6,7,8]
- "10 12 14:16" -> [10,12,14,15,16]

Validation:
- Non-integer tokens are ignored with a warning in logs.
