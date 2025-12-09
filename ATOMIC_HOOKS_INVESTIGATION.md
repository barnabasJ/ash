# Investigation: after_transaction Hooks Not Working with atomic_batches Strategy

## Summary

When passing a **list of records** (not a query) to `bulk_destroy` with `atomic_batches` strategy, `after_transaction` hooks don't execute automatically without explicit `return_records?: true`. This is because an optimization (`select([])`) prevents records from being loaded even though hooks need them.

## Background

- **Branch**: `atomic-after-transaction`
- **Latest commit**: `feat: enable after_transaction hooks in atomic bulk operations`
- The commit added support for `after_transaction` hooks in atomic bulk UPDATE and DESTROY operations
- Tests were added but they ALL explicitly set `return_records?: true`
- **Missing test coverage**: List-based atomic_batches with hooks relying on automatic `return_records?` calculation

## The Problem

### Code Flow

1. When you pass a list of records like `[post1, post2, post3]` with `strategy: [:atomic_batches]`
2. Goes through `do_atomic_batches` in `lib/ash/actions/destroy/bulk.ex:1034`
3. For each batch, creates a query by primary keys (line 1047-1064)
4. **Line 1064**: Applies `Ash.Query.select([])` - **THIS IS THE BUG**
5. Even though `return_records?: true` is calculated based on hooks (line 593-596)
6. The empty select prevents fields from being loaded
7. Data layer returns 0 records or records with no fields
8. Hooks can't execute because there are no records

### Key Code Locations

**Hook calculation (WORKS):**
```elixir
# lib/ash/actions/destroy/bulk.ex:593-596
return_records? =
  has_after_batch_hooks? || opts[:notify?] || opts[:return_records?] ||
    !Enum.empty?(atomic_changeset.after_action) ||
    !Enum.empty?(atomic_changeset.after_transaction)
```

**The bug (empty select):**
```elixir
# lib/ash/actions/destroy/bulk.ex:1064
|> Ash.Query.select([])  # <-- Blocks records from being loaded!
```

**Hook execution (FAILS):**
```elixir
# lib/ash/actions/destroy/bulk.ex:314-352
# Tries to execute hooks on bulk_result.records, but it's empty!
```

## Why Existing Tests Didn't Catch This

All existing ash tests either:
1. Use **queries** (not lists) - these go through `do_atomic_query`, not `do_atomic_batches`
2. **Explicitly set `return_records?: true`** - bypasses the automatic calculation
3. Don't test hooks with atomic_batches strategy

## Differences: after_action vs after_transaction

- **after_action hooks**: Execute INSIDE `do_atomic_query` (lines 683-709) on raw data layer results
  - Can work even with empty select because they run earlier
  
- **after_transaction hooks**: Execute OUTSIDE `do_atomic_query` (lines 314-352) on `bulk_result.records`
  - Fail because bulk_result.records is empty due to `select([])`

## Tests Added

### Test 1: after_transaction with atomic_batches (FAILS ❌)
**Location**: `test/actions/bulk/bulk_destroy_test.exs:1101`

### Test 2: after_action with atomic_batches (PASSES ✅)
**Location**: `test/actions/bulk/bulk_destroy_test.exs:1135`

## The Fix

**Location**: `lib/ash/actions/destroy/bulk.ex:1064`

Replace:
```elixir
|> Ash.Query.select([])
```

With:
```elixir
|> then(fn query ->
  # Only use empty select if we don't need records for hooks
  if Enum.empty?(atomic_changeset.after_transaction) &&
       Enum.empty?(atomic_changeset.after_action) do
    Ash.Query.select(query, [])
  else
    query
  end
end)
```

## Current State

- Tests added but NOT YET PASSING
- Fix identified but NOT YET APPLIED
- Debug output still in place
- Same issue exists in bulk UPDATE at `lib/ash/actions/update/bulk.ex:1349`

## Next Steps

1. Apply the fix at `lib/ash/actions/destroy/bulk.ex:1064`
2. Verify both tests pass
3. Apply same fix to bulk UPDATE
4. Remove debug output
5. Run full test suite
6. Commit changes

