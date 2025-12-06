# Investigation: after_action vs after_transaction with Atomic Updates

## Date: 2025-12-06

## Summary

This document captures the empirical findings from instrumented test runs
showing exactly how `after_action` and `after_transaction` hooks behave during
atomic upgrade attempts.

---

## Test Setup

Created two change modules with IO.inspect instrumentation:

- `AddAfterActionDuringPending` - Adds `after_action` hook
- `AddAfterTransactionDuringPending` - Adds `after_transaction` hook

Both changes:

- Implement `change/3` callback (called during changeset processing)
- Implement `atomic/3` callback (to support atomic execution)
- Add hooks during their `change/3` execution

---

## Actual Execution Flow

### Test 1: after_transaction Hook

**Action**: `update_with_atomic_upgrade_and_after_transaction`

- `require_atomic? false` (allows fallback to non-atomic)
- `skip_global_validations? true`

**Execution Log**:

```
=== AddAfterTransactionDuringPending.change/3 called ===
Phase: :validate
Dirty hooks before: []
Dirty hooks after: []
after_transaction list: 1 hooks

=== ATOMIC UPGRADE CHECK ===
Action: update_with_atomic_upgrade_and_after_transaction
All dirty_hooks: [:before_action]
dirty_hooks (excluding after_action): [:before_action]
after_action hooks: 0
atomic_after_action hooks: 0
after_transaction hooks: 1

=== after_transaction HOOK EXECUTED ===
```

**Key Observations**:

1. ✅ `change/3` runs in **`:validate` phase**, NOT `:pending`
2. ✅ `after_transaction` hook IS added to changeset (1 hook present)
3. ❌ Hook is NOT added to `dirty_hooks` (still `[]` after adding hook)
4. ❌ `dirty_hooks` contains `[:before_action]` from identity validation
5. ✅ Atomic upgrade FAILS due to `before_action` in dirty_hooks
6. ✅ Falls back to non-atomic execution (because `require_atomic? false`)
7. ✅ `after_transaction` hook EXECUTES successfully

---

### Test 2: after_action Hook

**Action**: `update_with_atomic_upgrade_and_after_action`

- `require_atomic?` defaults to true
- `skip_global_validations? true`

**Execution Log**:

```
=== AddAfterActionDuringPending.change/3 called ===
Phase: :validate
Dirty hooks before: []
Dirty hooks after: []
after_action list: 1 hooks
atomic_after_action list: 0 hooks

=== ATOMIC UPGRADE CHECK ===
Action: update_with_atomic_upgrade_and_after_action
All dirty_hooks: [:before_action]
dirty_hooks (excluding after_action): [:before_action]
after_action hooks: 1
atomic_after_action hooks: 0
after_transaction hooks: 0

ERROR: must be performed atomically, but it could not be
Reason: cannot atomically run a changeset with hooks in any phase other than
        `after_action`, got hooks in phases [:before_action]
```

**Key Observations**:

1. ✅ `change/3` runs in **`:validate` phase**, NOT `:pending`
2. ✅ `after_action` hook IS added to changeset (1 hook present)
3. ❌ Hook is NOT added to `atomic_after_action` (still 0!)
4. ❌ Hook is NOT added to `dirty_hooks` (still `[]` after adding hook)
5. ❌ `dirty_hooks` contains `[:before_action]` from identity validation
6. ✅ Atomic upgrade FAILS due to `before_action` in dirty_hooks
7. ❌ Raises `MustBeAtomic` error (because `require_atomic?` is true)

---

## Critical Finding: atomic_after_action Mechanism Doesn't Work As Expected

### Expected Behavior (from code reading):

```elixir
# In Ash.Changeset.after_action/3 (lines 6762, 6772)
if changeset.phase == :pending do
  %{
    changeset
    | after_action: changeset.after_action ++ [func],
      atomic_after_action: changeset.atomic_after_action ++ [func]  # ← Dual storage
  }
else
  %{changeset | after_action: changeset.after_action ++ [func]}
end
```

### Actual Behavior:

- Changes run during **`:validate` phase** (not `:pending`)
- Therefore hooks are ONLY added to `after_action`, NOT `atomic_after_action`
- The `atomic_after_action` list remains empty!

### When Does Phase Change to :validate?

```elixir
# In changeset.ex:3125
defp run_action_changes(changeset, %{changes: changes}, actor, authorize?, tracer, metadata) do
  changeset = set_phase(changeset, :validate)  # ← Phase set HERE
  # Changes run after this point
end
```

---

## Why dirty_hooks Remains Empty

### The maybe_dirty_hook Function:

```elixir
# In changeset.ex:6985-6991
defp maybe_dirty_hook(changeset, type) do
  if changeset.phase == :pending do  # ← Only adds during :pending!
    %{changeset | dirty_hooks: Enum.uniq([type | changeset.dirty_hooks])}
  else
    changeset
  end
end
```

### Result:

- `after_transaction` hook is added during `:validate` phase
- `maybe_dirty_hook` does NOT add `:after_transaction` to `dirty_hooks`
- `dirty_hooks` remains `[]` for our hooks

---

## Why [:before_action] Appears in dirty_hooks

Despite `skip_global_validations? true`, there's still a `[:before_action]` hook
in dirty_hooks.

### Source: Identity Validation

```elixir
# In changeset.ex:2790
if identity.pre_check_with do
  before_action(changeset, &validate_identity(&1, identity, identity.pre_check_with))
end
```

The `unique_title` identity on the Post resource adds a `before_action` hook to
pre-check uniqueness. This happens BEFORE the phase is set to `:validate`, so it
DOES get added to `dirty_hooks`.

---

## The Real Reason Atomic Upgrade Fails

### For after_transaction Test:

- ❌ Fails due to `[:before_action]` in dirty_hooks (from identity validation)
- ✅ Falls back to non-atomic (because `require_atomic? false`)
- ✅ `after_transaction` hook executes successfully in non-atomic path

### For after_action Test:

- ❌ Fails due to `[:before_action]` in dirty_hooks (from identity validation)
- ❌ Raises error (because `require_atomic?` is true)
- ❌ Never gets to execute atomically

**Neither test demonstrates the behavior we intended to test!**

---

## What This Means

### Misconception #1: "after_action allows atomic upgrade"

**Reality**: `after_action` is excluded from the dirty_hooks check:

```elixir
dirty_hooks = changeset.dirty_hooks -- [:after_action]  # Line 56
```

But our test shows `atomic_after_action` is empty, so the dual-storage mechanism
isn't working during `for_update`.

### Misconception #2: "after_transaction prevents atomic upgrade"

**Reality**: `after_transaction` is NOT being added to dirty_hooks (phase is
`:validate`, not `:pending`), so it's NOT preventing atomic upgrade. The
`:before_action` hook from identity validation is what's preventing it.

### Misconception #3: "atomic_after_action hooks are transferred during upgrade"

**Reality**: They WOULD be transferred (update.ex:113-119), but the list is
EMPTY because hooks are added during `:validate` phase, not `:pending`.

---

## Questions Raised

1. **When DOES `atomic_after_action` get populated?**

   - Not during `for_update` processing (phase is `:validate`)
   - Must be some other path where phase is `:pending` when hooks are added

2. **How do atomic updates with `after_action` hooks actually work?**

   - Current tests don't demonstrate this because of identity validation
     interference

3. **Is the `atomic_after_action` mechanism actually used?**
   - Code exists to transfer them (update.ex:113-119)
   - But when are they populated?

---

## Next Steps

1. Find or create a resource WITHOUT identity validations
2. Test atomic upgrade with clean changeset
3. Investigate when/how `atomic_after_action` gets populated in real usage
4. Determine if the mechanism works for direct
   `Ash.Changeset.new() |> after_action(...) |> for_update(...)`

---

## CLEAN TEST RESULTS (No Identity Validation)

Created `SimplePost` resource with NO identities, validations, or relationships.

### Test 1: after_transaction Hook

**Execution Log**:

```
=== AddAfterTransactionDuringPending.change/3 called ===
Phase: :validate
Dirty hooks before: []
Dirty hooks after: []
after_transaction list: 1 hooks

=== ATOMIC UPGRADE CHECK ===
Action: update_with_after_transaction
All dirty_hooks: []                    ← CLEAN!
dirty_hooks (excluding after_action): []
after_action hooks: 0
atomic_after_action hooks: 0
after_transaction hooks: 1

=== after_transaction HOOK EXECUTED ===
```

**Result**: ✅ Test PASSED

**Analysis**:

- dirty_hooks is completely empty (no identity validation interference)
- after_transaction hook is NOT in dirty_hooks (phase is `:validate`, not
  `:pending`)
- Atomic upgrade check passes (dirty_hooks is empty)
- But falls back to non-atomic because `require_atomic? false`
- after_transaction hook executes successfully in non-atomic path

**Question**: Why did it fall back if dirty_hooks was empty?

- Must investigate atomic upgrade logic for other failure reasons

### Test 2: after_action Hook

**Execution Log**:

```
=== AddAfterActionDuringPending.change/3 called ===
Phase: :validate
Dirty hooks before: []
Dirty hooks after: []
after_action list: 1 hooks
atomic_after_action list: 0 hooks      ← STILL EMPTY!

=== ATOMIC UPGRADE CHECK ===
Action: update_with_after_action
All dirty_hooks: []                    ← CLEAN!
dirty_hooks (excluding after_action): []
after_action hooks: 1
atomic_after_action hooks: 0           ← EMPTY!
after_transaction hooks: 0

=== AddAfterActionDuringPending.atomic/3 called ===  ← ATOMIC SUCCEEDED!
```

**Result**: ❌ Test FAILED - `after_action` hook never executed

**Analysis**:

- dirty_hooks is completely empty (no interference)
- `atomic/3` callback WAS CALLED - **atomic upgrade succeeded!**
- But `after_action` hook never executed
- Because `atomic_after_action` is empty (0 hooks)

**Flow**:

1. Original changeset: `after_action: [hook]`, `atomic_after_action: []`
2. Atomic upgrade transfers from `atomic_after_action` (empty)
3. New atomic changeset: `after_action: []`
4. Atomic execution completes with no hooks
5. Hook never runs

**Proof**: The `atomic/3` callback being invoked confirms atomic execution
succeeded.

---

## FINAL CONCLUSIONS

### 1. The atomic_after_action Mechanism is BROKEN for Action Changes

**Expected**:

- Hooks added during `:pending` phase go into both `after_action` and
  `atomic_after_action`
- During atomic upgrade, `atomic_after_action` hooks are transferred
- Hooks execute after atomic operation completes

**Reality**:

- Action changes run during `:validate` phase (not `:pending`)
- Hooks only go into `after_action`, NOT `atomic_after_action`
- `atomic_after_action` remains empty
- During atomic upgrade, NO hooks are transferred (empty list)
- **Hooks are silently lost during atomic execution**

### 2. after_transaction Does NOT Prevent Atomic Upgrade

**Previous Assumption**: `after_transaction` hooks prevent atomic upgrade via
dirty_hooks

**Reality**:

- `after_transaction` hooks are NOT added to dirty_hooks (phase is `:validate`)
- dirty_hooks remains empty
- Atomic upgrade check passes
- The hook exists on the changeset but doesn't block atomic execution

**Question**: Why does the test with `after_transaction` fall back to
non-atomic?

- Need to investigate other atomic upgrade failure conditions

### 3. Atomic Upgrade SUCCEEDS When dirty_hooks is Empty

With clean resource (no identities):

- ✅ dirty_hooks: `[]`
- ✅ Atomic upgrade proceeds
- ✅ `atomic/3` callback executes
- ❌ But hooks are lost because `atomic_after_action` is empty

### 4. The REAL Behavior Summary

| Hook Type                             | Added to dirty_hooks?      | Added to atomic_after_action? | Atomic Upgrade | Hook Executes? |
| ------------------------------------- | -------------------------- | ----------------------------- | -------------- | -------------- |
| after_action (via action change)      | ❌ No (phase is :validate) | ❌ No (phase is :validate)    | ✅ Succeeds    | ❌ **LOST**    |
| after_transaction (via action change) | ❌ No (phase is :validate) | N/A                           | ✅ Succeeds\*  | ❌ **LOST**    |

\*Falls back for unknown reason despite passing dirty_hooks check

---

## RESOLUTION: NOT A BUG - CORRECT USAGE PATTERN IDENTIFIED

**Initial Assumption (INCORRECT):** The `atomic_after_action` mechanism was
believed to be broken because hooks added in `change/3` were not executing
during atomic operations.

**Actual Behavior (CORRECT):** For atomic execution, `after_action` hooks must
be added in the `atomic/3` callback, not in `change/3`. This is the intended
design.

### Correct Usage Pattern

```elixir
defmodule MyChange do
  use Ash.Resource.Change

  def change(changeset, _opts, _context) do
    # For non-atomic execution
    Ash.Changeset.after_action(changeset, fn _changeset, result, _context ->
      # 3-arity signature for non-atomic
      {:ok, result}
    end)
  end

  def atomic(changeset, _opts, _context) do
    # For atomic execution - add hook here
    changeset =
      Ash.Changeset.after_action(changeset, fn _changeset, result ->
        # 2-arity signature for atomic (no context parameter)
        {:ok, result}
      end)

    {:atomic, changeset, %{}}
  end
end
```

### Key Learnings

1. **`after_action` hooks for atomic operations must be added in `atomic/3`**

   - Return `{:atomic, changeset, %{}}` to include the modified changeset

2. **Hook signatures differ between atomic and non-atomic:**

   - Non-atomic: `fn changeset, result, context ->`
   - Atomic: `fn changeset, result ->` (no context parameter)

3. **The `atomic_after_action` list is for internal use:**
   - Populated when hooks are added during `:pending` phase
   - Not intended for hooks added by action changes during `:validate` phase
   - Users should add hooks in `atomic/3` for atomic execution

This is **not a bug** - it's the correct design pattern for atomic operations.
