# proof add

Add a proof (delegation) to this agent.

Parses a delegation from a data string or file path and adds it to the local proof store. The delegation's audience must match the local agent's DID.

## Usage

```
guppy proof add <data-or-path>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `data-or-path` | Delegation data (inline) or path to a CAR file containing the delegation |

## Example

Add a delegation from a file:

```bash
guppy proof add delegation.car
```

## What's Next

Once you have a proof for a space, you can upload to it:

```bash
guppy upload <space>
```
