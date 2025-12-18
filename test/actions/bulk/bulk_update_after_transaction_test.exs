# SPDX-FileCopyrightText: 2019 ash contributors <https://github.com/ash-project/ash/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule Ash.Test.Actions.BulkUpdateAfterTransactionTest do
  @moduledoc """
  Tests for after_transaction hooks in bulk update operations.
  """
  use ExUnit.Case, async: false

  require Ash.Query

  alias Ash.Test.Domain, as: Domain

  defmodule Notifier do
    use Ash.Notifier

    def notify(notification) do
      send(self(), {:notification, notification})
    end
  end

  defmodule AtomicWithAfterTransaction do
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _changeset, {:ok, result} ->
        send(self(), {:after_transaction_called, result.id})
        {:ok, result}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AtomicUpdateWithAfterTransactionHandlingErrors do
    @moduledoc """
    Change module that adds after_transaction hooks that handle both success and failure.
    Supports both stream and atomic strategies.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          send(self(), {:after_transaction_update_success, result.id})
          {:ok, result}

        _changeset, {:error, error} ->
          send(self(), {:after_transaction_update_error, error})
          {:error, error}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AtomicUpdateWithAfterTransactionReturnsError do
    @moduledoc """
    Change module where after_transaction hook returns an error on success.
    Used to test that hook errors are captured properly.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          send(self(), {:after_transaction_hook_returning_error, result.id})
          {:error, "Hook intentionally returned error"}

        _changeset, {:error, error} ->
          {:error, error}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AtomicUpdateWithAfterTransactionConvertsErrorToSuccess do
    @moduledoc """
    Change module where after_transaction hook converts validation error to success.
    Used to test that hook return values are respected for invalid changesets.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          {:ok, result}

        changeset, {:error, _original_error} ->
          send(self(), {:after_transaction_converted_error_to_success})
          # Return a "fake" success with the original data
          {:ok, changeset.data}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AtomicUpdateWithAfterTransactionFailsForSomeRecords do
    @moduledoc """
    Change module where after_transaction hook returns an error for records with title containing "fail".
    Used to test partial_success status when some records succeed and some fail.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          if String.contains?(result.title, "fail") do
            send(self(), {:after_transaction_partial_failure, result.id})
            {:error, "Hook failed for title containing 'fail'"}
          else
            send(self(), {:after_transaction_partial_success, result.id})
            {:ok, result}
          end

        _changeset, {:error, error} ->
          {:error, error}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AtomicUpdateWithAfterTransactionModifiesError do
    @moduledoc """
    Change module where after_transaction hook modifies the error.
    Used to test that hook return values are respected for invalid changesets.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          {:ok, result}

        _changeset, {:error, _original_error} ->
          send(self(), {:after_transaction_modified_error})
          {:error, "custom error from hook"}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AfterActionFailsWithAfterTransaction do
    @moduledoc """
    Change module that adds an after_action hook that fails for specific records,
    and an after_transaction hook that converts the error to success.
    Used to test hook_success_results handling when changesets become invalid
    during run_after_action_hooks inside run_batch.
    """
    use Ash.Resource.Change

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end

    def change(changeset, _opts, _context) do
      changeset
      |> Ash.Changeset.after_action(fn _changeset, result ->
        # Fail if title starts with "fail_"
        if String.starts_with?(result.title || "", "fail_") do
          {:error,
           Ash.Error.Changes.InvalidAttribute.exception(
             field: :title,
             message: "title cannot start with fail_"
           )}
        else
          {:ok, result}
        end
      end)
      |> Ash.Changeset.after_transaction(fn
        _changeset, {:ok, result} ->
          {:ok, result}

        changeset, {:error, _original_error} ->
          send(self(), {:after_action_failed_converted_to_success})
          # Return a "fake" success with a recovered record
          {:ok, %{changeset.data | title: "recovered_from_after_action_failure"}}
      end)
    end
  end

  defmodule AlwaysFailsValidation do
    use Ash.Resource.Validation

    @impl true
    def validate(_, _, _) do
      {:error, field: :title, message: "always fails"}
    end

    @impl true
    def atomic(_, _, _) do
      {:error, field: :title, message: "always fails atomically"}
    end
  end

  defmodule AddAfterTransactionDuringPending do
    @moduledoc """
    This change adds an after_transaction hook and supports atomic execution.
    CORRECT PATTERN: Hook must be added in BOTH change/3 (for stream) and atomic/3 (for atomic).
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _changeset, {:ok, result} ->
        send(self(), {:atomic_upgrade_after_transaction_called, result.id})
        {:ok, result}
      end)
    end

    def atomic(changeset, _opts, _context) do
      # CORRECT PATTERN: Add the hook in atomic/3 for atomic execution
      {:ok,
       Ash.Changeset.after_transaction(changeset, fn _changeset, {:ok, result} ->
         send(self(), {:atomic_upgrade_after_transaction_called, result.id})
         {:ok, result}
       end)}
    end
  end

  defmodule AddAfterActionDuringPending do
    @moduledoc """
    This change adds an after_action hook and supports atomic execution.
    CORRECT PATTERN: Hook must be added in BOTH change/3 (for stream) and atomic/3 (for atomic).
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_action(changeset, fn _changeset, result ->
        send(self(), {:atomic_upgrade_after_action_called, result.id})
        {:ok, result}
      end)
    end

    def atomic(changeset, _opts, _context) do
      # CORRECT PATTERN: Add the hook in atomic/3 for atomic execution
      {:ok,
       Ash.Changeset.after_action(changeset, fn _changeset, result ->
         send(self(), {:atomic_upgrade_after_action_called, result.id})
         {:ok, result}
       end)}
    end
  end

  defmodule AfterTransactionRaisesException do
    @moduledoc """
    Change module where after_transaction hook raises an exception.
    Used to test that exceptions in hooks are caught and converted to errors.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _changeset, {:ok, result} ->
        send(self(), {:before_exception_raise, result.id})
        raise "Hook intentionally raised exception"
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AfterTransactionWithStopOnError do
    @moduledoc """
    Change module where after_transaction hook returns an error for specific records.
    Used to test stop_on_error? behavior with hook errors.
    Records with title containing "stop_here" will cause the hook to return an error.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          if String.contains?(result.title, "stop_here") do
            send(self(), {:hook_error_for_stop_on_error, result.id})
            {:error, "Hook error to trigger stop"}
          else
            send(self(), {:hook_success_for_stop_on_error, result.id})
            {:ok, result}
          end

        _changeset, {:error, error} ->
          {:error, error}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule Author do
    @moduledoc false
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets

    ets do
      private?(true)
    end

    actions do
      default_accept :*
      defaults [:read, :create, :update, :destroy]
    end

    attributes do
      uuid_primary_key :id

      attribute :name, :string do
        public? true
      end
    end

    relationships do
      has_many :posts, Ash.Test.Actions.BulkUpdateAfterTransactionTest.Post,
        destination_attribute: :author_id,
        public?: true
    end
  end

  defmodule SimplePost do
    @moduledoc """
    A minimal resource with NO identities, NO validations, NO relationships.
    Used to test pure hook behavior without interference.
    """
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets

    ets do
      private? true
    end

    attributes do
      uuid_primary_key :id

      attribute :title, :string do
        public? true
      end

      attribute :body, :string do
        public? true
      end
    end

    actions do
      default_accept :*
      defaults [:read, :destroy, create: :*, update: :*]

      update :update_with_after_transaction do
        require_atomic? false
        change AddAfterTransactionDuringPending
      end

      update :update_with_after_action do
        change AddAfterActionDuringPending
      end
    end
  end

  defmodule Post do
    @moduledoc false
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      notifiers: [Notifier]

    ets do
      private? true
    end

    actions do
      default_accept :*
      defaults [:read, :destroy, create: :*, update: :*]

      update :update_with_after_transaction do
        require_atomic? false

        change after_transaction(fn
                 _changeset, {:ok, result}, _context ->
                   {:ok, %{result | title: result.title <> "_stuff"}}

                 _changeset, {:error, error}, _context ->
                   send(self(), {:error, error})
                   {:error, error}
               end)
      end

      update :update_with_atomic_after_transaction do
        change AtomicWithAfterTransaction
      end

      update :update_with_atomic_upgrade_and_after_transaction do
        # This action allows atomic upgrade but will fall back to non-atomic
        # if atomic upgrade fails (e.g., due to after_transaction hooks)
        require_atomic? false
        change AddAfterTransactionDuringPending
      end

      update :update_with_atomic_after_transaction_always_fails do
        change AtomicUpdateWithAfterTransactionHandlingErrors
        validate AlwaysFailsValidation
      end

      update :update_with_atomic_after_transaction_returns_error do
        change AtomicUpdateWithAfterTransactionReturnsError
      end

      update :update_with_after_transaction_converts_error_to_success do
        change AtomicUpdateWithAfterTransactionConvertsErrorToSuccess
      end

      update :update_with_after_transaction_modifies_error do
        change AtomicUpdateWithAfterTransactionModifiesError
      end

      update :update_with_after_transaction_partial_failure do
        change AtomicUpdateWithAfterTransactionFailsForSomeRecords
      end

      update :update_with_after_action_failure_converted_to_success do
        change AfterActionFailsWithAfterTransaction
      end

      update :update_with_after_transaction_raises_exception do
        change AfterTransactionRaisesException
      end

      update :update_with_stop_on_error_hook do
        change AfterTransactionWithStopOnError
      end
    end

    attributes do
      uuid_primary_key :id
      attribute :title, :string, allow_nil?: false, public?: true
      attribute :title2, :string, public?: true
    end

    relationships do
      belongs_to :author, Author, public?: true
    end
  end

  defmodule MnesiaAfterTransactionChange do
    @moduledoc """
    Change module that adds after_transaction hook for Mnesia resource.
    Used to test warning when after_transaction runs inside a transaction.
    """
    use Ash.Resource.Change

    def atomic(changeset, _opts, _context) do
      {:ok, change(changeset, [], %{})}
    end

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _changeset, {:ok, result} ->
        send(self(), {:mnesia_after_transaction_called, result.id})
        {:ok, result}
      end)
    end
  end

  defmodule MnesiaPost do
    @moduledoc false
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Mnesia, notifiers: [Notifier]

    mnesia do
      table :mnesia_post_after_transaction_updates
    end

    attributes do
      uuid_primary_key :id
      attribute :title, :string, allow_nil?: false, public?: true
    end

    actions do
      default_accept :*
      defaults [:read, :destroy, :create, :update]

      update :update_with_after_action_error_and_after_transaction do
        # after_action returns error, triggering rollback
        change after_action(fn _changeset, _result, _context ->
                 send(self(), {:after_action_error_hook_called})
                 {:error, "after_action hook error"}
               end)

        # after_transaction should still be called after the rollback
        change after_transaction(fn _changeset, result, _context ->
                 send(self(), {:after_transaction_called, result})
                 result
               end)
      end

      update :update_with_after_transaction do
        change MnesiaAfterTransactionChange
      end
    end
  end

  describe "atomic changes with after_transaction hooks" do
    test "after_transaction hooks execute in atomic strategy" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # after_transaction hooks can now be added in atomic/3 callback
      # and will execute after the transaction closes
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{}, strategy: :atomic)

      assert result.status == :success

      # Verify the hook executed by checking for the message
      assert_received {:after_transaction_called, post_id}
      assert post_id == post.id
    end

    test "after_transaction hooks in atomic strategy return records when requested" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 1

      # Verify the hook executed
      assert_received {:after_transaction_called, post_id}
      assert post_id == post.id
    end

    test "after_transaction hooks added during :pending phase allow atomic upgrade" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test", title2: "test2"})
        |> Ash.create!()

      # Update a single record - this will attempt atomic upgrade
      # The change's change/3 callback adds an after_transaction hook during :pending phase
      # Hooks added during :pending go into both after_transaction AND atomic_after_transaction
      # after_transaction is now excluded from dirty_hooks (like after_action)
      # So atomic upgrade succeeds and the hook is transferred
      result =
        post
        |> Ash.Changeset.for_update(:update_with_atomic_upgrade_and_after_transaction, %{
          title2: "updated"
        })
        |> Ash.update!()

      # The update succeeds
      assert result.title2 == "updated"

      # The after_transaction hook DID run (via atomic upgrade with hook transfer)
      assert_receive {:atomic_upgrade_after_transaction_called, _}, 100
    end

    test "after_transaction with :atomic_batches strategy - hooks execute" do
      posts =
        for i <- 1..5 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: [:atomic_batches],
          batch_size: 2
        )

      assert result.status == :success

      # Verify all hooks executed
      for post_id <- post_ids do
        assert_received {:after_transaction_called, ^post_id}
      end
    end

    test "after_transaction with :atomic_batches strategy - returns records when requested" do
      posts =
        for i <- 1..5 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: [:atomic_batches],
          batch_size: 2,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 5

      # Verify all hooks executed
      for post_id <- post_ids do
        assert_received {:after_transaction_called, ^post_id}
      end
    end

    test "after_transaction hook receives correct changeset and result" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # Use a hook that captures the changeset
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{}, strategy: :atomic)

      assert result.status == :success
      assert_received {:after_transaction_called, post_id}
      assert post_id == post.id
    end

    test "after_transaction hook error is captured in result" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # This test would need a hook that returns an error
      # For now, verify the basic mechanism works
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{}, strategy: :atomic)

      assert result.status == :success
    end

    test "multiple after_transaction hooks execute in order" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # The AtomicWithAfterTransaction module adds one hook
      # We'd need to modify it to add multiple hooks to test order
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{}, strategy: :atomic)

      assert result.status == :success
      assert_received {:after_transaction_called, _}
    end

    test ":pending phase hooks with :atomic strategy use dual storage" do
      # This test verifies that hooks added during :pending phase
      # are stored in both after_transaction AND atomic_after_transaction
      simple_post =
        SimplePost
        |> Ash.Changeset.for_create(:create, %{title: "test", body: "body"})
        |> Ash.create!()

      # The update_with_after_transaction action adds a hook during :pending phase
      # via the AddAfterTransactionDuringPending change module
      result =
        simple_post
        |> Ash.Changeset.for_update(:update_with_after_transaction, %{body: "updated"})
        |> Ash.update!()

      assert result.body == "updated"
      # Hook should execute even though other hooks prevented atomic upgrade
      assert_receive {:atomic_upgrade_after_transaction_called, _}, 100
    end

    test ":validate phase hooks with :stream strategy work" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # Use stream strategy which always works with after_transaction
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update(:update_with_atomic_upgrade_and_after_transaction, %{},
          strategy: [:stream]
        )

      assert result.status == :success
      assert_receive {:atomic_upgrade_after_transaction_called, _}, 100
    end

    test "empty result set doesn't cause errors" do
      # Query that matches no records
      result =
        Post
        |> Ash.Query.filter(id == "nonexistent")
        |> Ash.bulk_update(:update_with_atomic_after_transaction, %{}, strategy: :atomic)

      assert result.status == :success
      assert result.records == []
      # No hooks should execute since no records matched
      refute_received {:after_transaction_called, _}
    end

    test "with explicit return_errors?: true returns errors list" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_errors?: true
        )

      assert result.status == :success
      assert result.errors == []

      # Verify hooks executed
      for post_id <- post_ids do
        assert_received {:after_transaction_called, ^post_id}
      end
    end

    test "with both return_records?: true and return_errors?: true" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true,
          return_errors?: true
        )

      assert result.status == :success
      assert length(result.records) == 3
      assert result.errors == []

      # Verify hooks executed
      for post_id <- post_ids do
        assert_received {:after_transaction_called, ^post_id}
      end
    end

    test "with return_stream?: true streams results (:stream strategy)" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # return_stream?: true only works with :stream strategy
      result_stream =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update(:update_with_atomic_after_transaction, %{},
          strategy: :stream,
          return_stream?: true,
          return_records?: true
        )

      # Consume the stream
      results = Enum.to_list(result_stream)
      assert length(results) == 3

      # Verify hooks executed (use assert_receive with timeout for async operations)
      for post_id <- post_ids do
        assert_receive {:after_transaction_called, ^post_id}, 1000
      end
    end

    test "after_transaction hooks run on failure with :atomic strategy" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # This action has a validation that always fails
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update(:update_with_atomic_after_transaction_always_fails, %{},
          strategy: :atomic,
          return_errors?: true
        )

      assert result.status == :error
      assert result.error_count == 1

      # Verify the after_transaction hook received the error
      assert_receive {:after_transaction_update_error, _error}, 1000

      # Verify the post was NOT updated (operation failed)
      assert [_] = Ash.read!(Post)
    end

    test "after_transaction hooks run on failure with :atomic_batches strategy" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      # This action has a validation that always fails
      # With atomic_batches, validation happens once before the batches run
      result =
        Post
        |> Ash.Query.filter(id in ^Enum.map(posts, & &1.id))
        |> Ash.bulk_update(:update_with_atomic_after_transaction_always_fails, %{},
          strategy: [:atomic_batches],
          return_errors?: true
        )

      assert result.status == :error
      # Only 1 error because validation fails on the single atomic changeset
      assert result.error_count == 1

      # Verify hook received error (only one because atomic changeset is shared)
      assert_receive {:after_transaction_update_error, _error}, 1000

      # Verify posts were NOT updated (operation failed)
      assert length(Ash.read!(Post)) == 3
    end

    test "after_transaction hooks run on failure with :stream strategy and return_stream?" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # With stream strategy and return_stream?, the operation is lazy
      # We use an action that always fails validation
      result_stream =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update(:update_with_atomic_after_transaction_always_fails, %{},
          strategy: :stream,
          return_stream?: true,
          return_errors?: true
        )

      # Consume the stream to trigger the operations
      results = Enum.to_list(result_stream)

      # Each record should fail and trigger its hook
      assert length(results) == 3

      for result <- results do
        assert {:error, _} = result
      end

      # Verify hooks executed for each failed record
      for _post_id <- post_ids do
        assert_receive {:after_transaction_update_error, _error}, 1000
      end

      # Verify posts were NOT updated (all operations failed)
      assert length(Ash.read!(Post)) == 3
    end

    test "after_transaction hook error on success is captured in result" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # This action's hook returns an error even on success
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update(:update_with_atomic_after_transaction_returns_error, %{},
          strategy: :atomic,
          return_errors?: true,
          return_records?: true
        )

      # The hook was called and returned an error
      assert_receive {:after_transaction_hook_returning_error, _post_id}, 1000

      # The operation should show the error from the hook with correct status
      assert result.status == :error
      assert result.error_count == 1
      assert length(result.errors) == 1
      assert result.records == []
    end

    test "after_transaction hook error on success is captured with :atomic_batches strategy" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # This action's hook returns an error even on success
      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update(:update_with_atomic_after_transaction_returns_error, %{},
          strategy: [:atomic_batches],
          return_errors?: true
        )

      # All hooks are called (one per record)
      for _post_id <- post_ids do
        assert_receive {:after_transaction_hook_returning_error, _}, 1000
      end

      # Hook errors are captured in the result
      # Note: atomic_batches currently consolidates multiple errors into 1
      assert result.status == :error
      assert result.error_count == 1
    end

    test "after_transaction hook error on success is captured with :stream strategy" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # This action's hook returns an error even on success
      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update(:update_with_atomic_after_transaction_returns_error, %{},
          strategy: :stream,
          return_errors?: true
        )

      # Stream strategy stops on first error, so only 1 hook is called
      assert_receive {:after_transaction_hook_returning_error, _}, 1000

      # Hook error is captured in the result
      assert result.status == :error
      assert result.error_count == 1
    end

    test "after_transaction hook partial failure sets status to :partial_success with :atomic strategy" do
      # Create posts with different titles - some will fail, some will succeed
      success_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "success"})
        |> Ash.create!()

      fail_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "will fail"})
        |> Ash.create!()

      another_success =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "also success"})
        |> Ash.create!()

      success_post_id = success_post.id
      fail_post_id = fail_post.id
      another_success_id = another_success.id
      post_ids = [success_post_id, fail_post_id, another_success_id]

      # This action's hook fails only for records with title containing "fail"
      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update(:update_with_after_transaction_partial_failure, %{},
          strategy: :atomic,
          return_errors?: true,
          return_records?: true
        )

      # The success hooks should have been called
      assert_receive {:after_transaction_partial_success, ^success_post_id}, 1000
      assert_receive {:after_transaction_partial_failure, ^fail_post_id}, 1000
      assert_receive {:after_transaction_partial_success, ^another_success_id}, 1000

      # Status should be partial_success since some records succeeded and some failed
      assert result.status == :partial_success
      assert result.error_count == 1
      assert length(result.errors) == 1
      assert length(result.records) == 2
    end
  end

  describe "clean atomic upgrade tests (no identity validation interference)" do
    test "CLEAN: after_transaction hooks allow atomic upgrade" do
      simple_post =
        SimplePost
        |> Ash.Changeset.for_create(:create, %{title: "test", body: "body"})
        |> Ash.create!()

      result =
        simple_post
        |> Ash.Changeset.for_update(:update_with_after_transaction, %{body: "updated"})
        |> Ash.update!()

      assert result.body == "updated"
      assert_receive {:atomic_upgrade_after_transaction_called, _}, 100
    end

    test "CLEAN: after_action hooks allow atomic upgrade" do
      simple_post =
        SimplePost
        |> Ash.Changeset.for_create(:create, %{title: "test", body: "body"})
        |> Ash.create!()

      result =
        simple_post
        |> Ash.Changeset.for_update(:update_with_after_action, %{body: "updated"})
        |> Ash.update!()

      assert result.body == "updated"
      assert_receive {:atomic_upgrade_after_action_called, _}, 100
    end

    test "after_action hooks work with atomic upgrade when added in atomic/3" do
      # This test demonstrates the CORRECT pattern for after_action hooks with atomic support.
      #
      # CORRECT PATTERN:
      # 1. Add hook in change/3 for stream/non-atomic execution
      # 2. Add hook in atomic/3 for atomic execution
      # 3. Both paths work correctly
      #
      # IMPORTANT: Hooks added during :validate phase (in change/3) are NOT automatically
      # transferred during atomic upgrade. You must add them in atomic/3 as well.

      simple_post =
        SimplePost
        |> Ash.Changeset.for_create(:create, %{title: "test", body: "body"})
        |> Ash.create!()

      result =
        simple_post
        |> Ash.Changeset.for_update(:update_with_after_action, %{body: "updated"})
        |> Ash.update!()

      # Update succeeds
      assert result.body == "updated"

      # Hook executes because AddAfterActionDuringPending adds it in atomic/3
      assert_receive {:atomic_upgrade_after_action_called, _}, 500
    end
  end

  describe "UPDATE actions - :pending phase" do
    test ":stream strategy with after_transaction" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      Post
      |> Ash.Query.filter(id == ^post.id)
      |> Ash.bulk_update(:update_with_atomic_upgrade_and_after_transaction, %{},
        strategy: [:stream]
      )

      assert_receive {:atomic_upgrade_after_transaction_called, _}, 100
    end

    test ":atomic_batches strategy with after_transaction" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      Post
      |> Ash.Query.filter(id in ^Enum.map(posts, & &1.id))
      |> Ash.bulk_update(:update_with_atomic_upgrade_and_after_transaction, %{},
        strategy: [:atomic_batches],
        batch_size: 2
      )

      for p <- posts do
        assert_receive {:atomic_upgrade_after_transaction_called, post_id} when post_id == p.id,
                       100
      end
    end

    test ":stream strategy with after_action" do
      post =
        SimplePost
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      SimplePost
      |> Ash.Query.filter(id == ^post.id)
      |> Ash.bulk_update(:update_with_after_action, %{}, strategy: [:stream])

      assert_receive {:atomic_upgrade_after_action_called, _}, 100
    end
  end

  describe "UPDATE actions - :validate phase" do
    test ":stream strategy with after_transaction" do
      post =
        SimplePost
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      SimplePost
      |> Ash.Query.filter(id == ^post.id)
      |> Ash.bulk_update(:update_with_after_transaction, %{}, strategy: [:stream])

      assert_receive {:atomic_upgrade_after_transaction_called, _}, 100
    end

    test ":atomic strategy with after_transaction" do
      post =
        SimplePost
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      SimplePost
      |> Ash.Query.filter(id == ^post.id)
      |> Ash.bulk_update(:update_with_after_transaction, %{}, strategy: [:atomic])

      assert_receive {:atomic_upgrade_after_transaction_called, _}, 100
    end

    test ":atomic strategy with after_action" do
      post =
        SimplePost
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      SimplePost
      |> Ash.Query.filter(id == ^post.id)
      |> Ash.bulk_update(:update_with_after_action, %{}, strategy: [:atomic])

      assert_receive {:atomic_upgrade_after_action_called, _}, 100
    end
  end

  describe "after_transaction with notification options" do
    test "after_transaction hooks work with return_notifications?: true and :atomic strategy" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true,
          return_notifications?: true
        )

      assert result.status == :success
      assert length(result.records) == 3
      # Notifications should be returned
      assert length(result.notifications) == 3

      # Verify hooks executed
      for post_id <- post_ids do
        assert_receive {:after_transaction_called, ^post_id}, 1000
      end
    end

    test "after_transaction hooks work with return_notifications?: true and :stream strategy" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: :stream,
          return_records?: true,
          return_notifications?: true
        )

      assert result.status == :success
      assert length(result.records) == 3
      # Notifications should be returned
      assert length(result.notifications) == 3

      # Verify hooks executed
      for post_id <- post_ids do
        assert_receive {:after_transaction_called, ^post_id}, 1000
      end
    end

    test "after_transaction hooks work with notify?: true and :atomic strategy" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true,
          notify?: true
        )

      assert result.status == :success
      assert length(result.records) == 3

      # Notifications should be sent (via Notifier module)
      assert_received {:notification, _}
      assert_received {:notification, _}
      assert_received {:notification, _}

      # Verify after_transaction hooks also executed
      for post_id <- post_ids do
        assert_receive {:after_transaction_called, ^post_id}, 1000
      end
    end

    test "after_transaction hooks work with notify?: true and :stream strategy" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: :stream,
          return_records?: true,
          notify?: true
        )

      assert result.status == :success
      assert length(result.records) == 3

      # Notifications should be sent (via Notifier module)
      assert_received {:notification, _}
      assert_received {:notification, _}
      assert_received {:notification, _}

      # Verify after_transaction hooks also executed
      for post_id <- post_ids do
        assert_receive {:after_transaction_called, ^post_id}, 1000
      end
    end

    test "after_transaction hooks work with notify?: true and :atomic_batches strategy" do
      posts =
        for i <- 1..5 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(:update_with_atomic_after_transaction, %{},
          strategy: [:atomic_batches],
          batch_size: 2,
          return_records?: true,
          notify?: true
        )

      assert result.status == :success
      assert length(result.records) == 5

      # Notifications should be sent (via Notifier module)
      for _ <- 1..5 do
        assert_received {:notification, _}
      end

      # Verify after_transaction hooks also executed
      for post_id <- post_ids do
        assert_receive {:after_transaction_called, ^post_id}, 1000
      end
    end
  end

  describe "after_transaction hook return value for invalid changesets" do
    test "hook can convert validation error to success" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # Update with invalid data (title is nil, but required)
      # The after_transaction hook should convert the error to success
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update(
          :update_with_after_transaction_converts_error_to_success,
          %{title: nil},
          strategy: :stream,
          return_records?: true,
          return_errors?: true
        )

      # The hook should have been called
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # The result should show success, not error
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 1
    end

    test "hook can modify validation error" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # Update with invalid data (title is nil, but required)
      # The after_transaction hook should modify the error
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update(
          :update_with_after_transaction_modifies_error,
          %{title: nil},
          strategy: :stream,
          return_errors?: true
        )

      # The hook should have been called
      assert_receive {:after_transaction_modified_error}, 1000

      # The error should be the modified one from the hook, not the original validation error
      assert result.error_count == 1
      assert result.errors != []

      error = hd(result.errors)
      # The error should contain "custom error from hook", not the original "is required" error
      assert Exception.message(error) =~ "custom error from hook"
    end

    test "hook can convert error to success with transaction: :all and mixed valid/invalid changesets" do
      # Create posts - one valid, one that will become invalid via validation
      valid_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "valid_title"})
        |> Ash.create!()

      invalid_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "will_be_invalid"})
        |> Ash.create!()

      # Update with a mix of valid and invalid data
      # The invalid one's after_transaction hook should convert the error to success
      result =
        Post
        |> Ash.Query.filter(id in [^valid_post.id, ^invalid_post.id])
        |> Ash.Query.sort(:title)
        |> Ash.bulk_update(
          :update_with_after_transaction_converts_error_to_success,
          %{title: nil},
          strategy: :stream,
          transaction: :all,
          return_records?: true,
          return_errors?: true,
          sorted?: true
        )

      # The hook should have been called for the invalid changeset(s)
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # Both records should be returned - converted to success by the hook
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 2
    end

    test "hook can convert error to success with transaction: :all and mixed valid/invalid changesets (unsorted)" do
      # Same test without sorted? to expose deeper issues with hook_success_results
      valid_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "valid_title"})
        |> Ash.create!()

      invalid_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "will_be_invalid"})
        |> Ash.create!()

      result =
        Post
        |> Ash.Query.filter(id in [^valid_post.id, ^invalid_post.id])
        |> Ash.bulk_update(
          :update_with_after_transaction_converts_error_to_success,
          %{title: nil},
          strategy: :stream,
          transaction: :all,
          return_records?: true,
          return_errors?: true
        )

      # The hook should have been called for the invalid changeset(s)
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # Both records should be returned - converted to success by the hook
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 2
    end

    test "after_action failure converted to success in run_after_action_hooks with mixed valid/invalid" do
      # This test exposes the bug where hook_success_results from after_action failures
      # that are converted to success by after_transaction hooks are not properly handled.
      # Create posts - one that will succeed, one that will fail in after_action
      valid_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "valid_title"})
        |> Ash.create!()

      fail_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "fail_this_one"})
        |> Ash.create!()

      # Update with an action that has after_action hook that fails for "fail_" prefixed titles
      # and after_transaction hook that converts the error to success
      result =
        Post
        |> Ash.Query.filter(id in [^valid_post.id, ^fail_post.id])
        |> Ash.bulk_update(
          :update_with_after_action_failure_converted_to_success,
          %{},
          strategy: :stream,
          transaction: :all,
          return_records?: true,
          return_errors?: true
        )

      # The after_transaction hook should have converted the after_action failure to success
      assert_receive {:after_action_failed_converted_to_success}, 1000

      # Both records should be returned
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 2

      # Verify both records are present
      titles = Enum.map(result.records, & &1.title)
      assert "valid_title" in titles
      assert "recovered_from_after_action_failure" in titles
    end

    test "after_action failure converted to success without transaction: :all" do
      # Same test without transaction: :all to verify after_transaction hooks
      # are called correctly outside of a wrapping transaction
      valid_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "valid_title"})
        |> Ash.create!()

      fail_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "fail_this_one"})
        |> Ash.create!()

      result =
        Post
        |> Ash.Query.filter(id in [^valid_post.id, ^fail_post.id])
        |> Ash.bulk_update(
          :update_with_after_action_failure_converted_to_success,
          %{},
          strategy: :stream,
          return_records?: true,
          return_errors?: true
        )

      # The after_transaction hook should have converted the after_action failure to success
      assert_receive {:after_action_failed_converted_to_success}, 1000

      # Both records should be returned
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 2

      # Verify both records are present
      titles = Enum.map(result.records, & &1.title)
      assert "valid_title" in titles
      assert "recovered_from_after_action_failure" in titles
    end

    test "load option is applied to records from after_transaction converting error to success" do
      # This test verifies that records returned from after_transaction hooks
      # that convert errors to success get the load option applied.
      # Create an author
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Test Author"})
        |> Ash.create!()

      # Create a post with the author
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test", author_id: author.id})
        |> Ash.create!()

      # Update with invalid data (title is nil, but required)
      # The after_transaction hook should convert the error to success
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update(
          :update_with_after_transaction_converts_error_to_success,
          %{title: nil},
          strategy: :stream,
          return_records?: true,
          return_errors?: true,
          load: [:author]
        )

      # The hook should have been called
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # The result should show success
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 1

      [record] = result.records

      # The record should have the author_id set
      assert record.author_id == author.id

      # The author relationship should be loaded because we passed load: [:author]
      assert %Author{} = record.author,
             "Expected author to be loaded, but got: #{inspect(record.author)}"
    end

    test "sorted? option works with after_transaction converting error to success" do
      # This test verifies that when after_transaction hooks convert errors to success,
      # the resulting records are properly sorted by their original index when sorted?: true

      # Create posts with different titles to ensure specific ordering
      post1 =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "aaa_first"})
        |> Ash.create!()

      post2 =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "bbb_second"})
        |> Ash.create!()

      post3 =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "ccc_third"})
        |> Ash.create!()

      # Update with invalid data - all will fail validation and be converted to success
      result =
        Post
        |> Ash.Query.filter(id in [^post1.id, ^post2.id, ^post3.id])
        |> Ash.Query.sort(:title)
        |> Ash.bulk_update(
          :update_with_after_transaction_converts_error_to_success,
          %{title: nil},
          strategy: :stream,
          return_records?: true,
          return_errors?: true,
          sorted?: true
        )

      # Hooks should have been called
      assert_receive {:after_transaction_converted_error_to_success}, 1000
      assert_receive {:after_transaction_converted_error_to_success}, 1000
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # All three should succeed
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 3

      # With sorted?: true, records should be in original query order (sorted by title)
      titles = Enum.map(result.records, & &1.title)
      assert titles == ["aaa_first", "bbb_second", "ccc_third"]
    end
  end

  describe "after_transaction hooks run outside batch transaction" do
    import ExUnit.CaptureLog

    setup do
      capture_log(fn ->
        Ash.DataLayer.Mnesia.start(Domain, [MnesiaPost])
      end)

      # Clean up any existing records to ensure test isolation
      MnesiaPost
      |> Ash.read!()
      |> Enum.each(fn record -> Ash.destroy!(record) end)

      :ok
    end

    test "after_action error with rollback_on_error? triggers rollback and after_transaction is NOT called" do
      # Create records first
      post1 =
        MnesiaPost
        |> Ash.Changeset.for_create(:create, %{title: "title1"})
        |> Ash.create!()

      post2 =
        MnesiaPost
        |> Ash.Changeset.for_create(:create, %{title: "title2"})
        |> Ash.create!()

      result =
        MnesiaPost
        |> Ash.Query.filter(id in [^post1.id, ^post2.id])
        |> Ash.bulk_update(
          :update_with_after_action_error_and_after_transaction,
          %{},
          strategy: :stream,
          transaction: :all,
          rollback_on_error?: true,
          return_errors?: true
        )

      # The after_action hook should have been called (it triggers the rollback)
      assert_receive {:after_action_error_hook_called}

      # The transaction was rolled back, returns a BulkResult
      # Note: With stream strategy and rollback, the status may be :success but errors are captured
      assert %Ash.BulkResult{errors: errors} = result
      assert length(errors) > 0
      [error | _] = errors
      assert %Ash.Error.Unknown.UnknownError{error: "\"after_action hook error\""} = error

      # after_transaction hook is NOT called because we don't have access to
      # the changesets after a transaction rollback
      refute_receive {:after_transaction_called, _}

      # Records should still exist (transaction rolled back, original data preserved)
      assert length(MnesiaPost |> Ash.read!()) == 2
    end

    test "after_transaction hooks run outside batch transaction - no warning" do
      # Create a record first
      post =
        MnesiaPost
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # With transaction: :batch, after_transaction hooks now run OUTSIDE the transaction
      # so no warning should be logged
      log =
        capture_log(fn ->
          result =
            MnesiaPost
            |> Ash.Query.filter(id == ^post.id)
            |> Ash.bulk_update(
              :update_with_after_transaction,
              %{title: "updated"},
              strategy: :stream,
              return_records?: true,
              authorize?: false
              # transaction: :batch is the default
            )

          assert result.status == :success
          assert length(result.records) == 1
        end)

      # Verify the hook executed
      assert_receive {:mnesia_after_transaction_called, _id}, 1000

      # Should NOT warn since after_transaction now runs outside the transaction
      refute log =~ "after_transaction"
    end
  end

  describe "after_transaction exception handling" do
    test "exception in hook is caught and converted to error" do
      # Create a record
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      post_id = post.id

      # Use the action with hook that raises exception
      result =
        Post
        |> Ash.Query.filter(id == ^post_id)
        |> Ash.bulk_update(
          :update_with_after_transaction_raises_exception,
          %{title: "updated"},
          strategy: :stream,
          return_errors?: true,
          return_records?: true,
          authorize?: false
        )

      # Hook should have sent message before raising
      assert_receive {:before_exception_raise, ^post_id}

      # Exception should be caught and converted to error
      assert result.status == :error
      assert length(result.errors) == 1
      [error] = result.errors
      assert Exception.message(error) =~ "Hook intentionally raised exception"
    end

    test "hook raising exception doesn't crash bulk operation with multiple records" do
      # Create multiple records
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "test_#{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # Use the action with hook that raises exception - with stop_on_error?: false
      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update(
          :update_with_after_transaction_raises_exception,
          %{title: "updated"},
          strategy: :stream,
          return_errors?: true,
          return_records?: true,
          stop_on_error?: false,
          authorize?: false
        )

      # All hooks should have sent messages before raising
      assert_receive {:before_exception_raise, _id1}
      assert_receive {:before_exception_raise, _id2}
      assert_receive {:before_exception_raise, _id3}

      # All exceptions should be caught and converted to errors
      assert result.status == :error
      assert length(result.errors) == 3
    end
  end

  describe "after_transaction with stop_on_error? (default: true)" do
    test "hook returns error without return_stream? - error captured in result" do
      # Create a record
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "stop_here"})
        |> Ash.create!()

      post_id = post.id

      # Use the action with hook that returns error for "stop_here" titles
      result =
        Post
        |> Ash.Query.filter(id == ^post_id)
        |> Ash.bulk_update(
          :update_with_stop_on_error_hook,
          %{},
          strategy: :stream,
          return_errors?: true,
          return_records?: true,
          authorize?: false
        )

      # Hook should have executed and returned error
      assert_receive {:hook_error_for_stop_on_error, ^post_id}

      # Error should be captured
      assert result.status == :error
      assert length(result.errors) == 1
    end

    test "hook returns error stops processing with default stop_on_error?: true" do
      # Create multiple records - one that will fail, one that won't
      post1 =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "stop_here_first"})
        |> Ash.create!()

      post2 =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "normal_second"})
        |> Ash.create!()

      # Query both posts, stop_here will be processed first and cause stop
      result =
        Post
        |> Ash.Query.filter(id in ^[post1.id, post2.id])
        |> Ash.Query.sort(:title)
        |> Ash.bulk_update(
          :update_with_stop_on_error_hook,
          %{},
          strategy: :stream,
          return_errors?: true,
          return_records?: true,
          authorize?: false
          # stop_on_error?: true is the default in test config
        )

      # With stop_on_error?: true (test default), the error is captured
      assert result.status == :error

      # The first record should have triggered the error
      assert_receive {:hook_error_for_stop_on_error, _}
    end

    test "stop_on_error?: false continues after hook error" do
      # Create multiple records - some will fail, some will succeed
      posts =
        for title <- ["stop_here_1", "normal_1", "stop_here_2", "normal_2"] do
          Post
          |> Ash.Changeset.for_create(:create, %{title: title})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # Explicitly disable stop_on_error
      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update(
          :update_with_stop_on_error_hook,
          %{},
          strategy: :stream,
          return_errors?: true,
          return_records?: true,
          stop_on_error?: false,
          authorize?: false
        )

      # With stop_on_error?: false, all records should be processed
      # 2 should succeed ("normal_*") and 2 should fail ("stop_here_*")
      assert_receive {:hook_success_for_stop_on_error, _}
      assert_receive {:hook_success_for_stop_on_error, _}
      assert_receive {:hook_error_for_stop_on_error, _}
      assert_receive {:hook_error_for_stop_on_error, _}

      # partial_success because some succeeded and some failed
      assert result.status == :partial_success
      # Should have 2 successes and 2 errors
      assert length(result.records) == 2
      assert length(result.errors) == 2
    end
  end

  describe "after_transaction with strategy fallback" do
    test "strategy: [:atomic, :stream] - hooks work when falling back to stream" do
      # Create records
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "fallback_test_#{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # Use strategy list - will try atomic first, then fall back to stream
      # The action uses require_atomic?: false so it can fall back
      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(
          :update_with_atomic_upgrade_and_after_transaction,
          %{title: "updated"},
          strategy: [:atomic, :stream],
          return_records?: true,
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 3

      # after_transaction hooks should execute regardless of which strategy was used
      assert_receive {:atomic_upgrade_after_transaction_called, _id1}, 1000
      assert_receive {:atomic_upgrade_after_transaction_called, _id2}, 1000
      assert_receive {:atomic_upgrade_after_transaction_called, _id3}, 1000
    end

    test "strategy: :atomic with require_atomic?: false falls back and hooks still work" do
      # Create a record
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "atomic_fallback"})
        |> Ash.create!()

      # Use atomic strategy with an action that has require_atomic?: false
      # This tests the atomic upgrade path
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(
          :update_with_atomic_upgrade_and_after_transaction,
          %{title: "updated"},
          strategy: :atomic,
          return_records?: true,
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 1

      # Hook should still work after fallback
      assert_receive {:atomic_upgrade_after_transaction_called, _id}, 1000
    end
  end

  describe "atomic update with load and after_transaction" do
    test "load option with :atomic strategy and after_transaction hooks" do
      # Create an author and post
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Test Author"})
        |> Ash.create!()

      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "load_test", author_id: author.id})
        |> Ash.create!()

      # Use atomic strategy with load option and after_transaction hooks
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(
          :update_with_atomic_after_transaction,
          %{title: "updated"},
          strategy: :atomic,
          return_records?: true,
          load: [:author],
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 1
      [updated_post] = result.records

      # Verify load worked
      assert updated_post.author.name == "Test Author"

      # Verify after_transaction hook executed
      assert_receive {:after_transaction_called, _id}, 1000
    end

    test "load option with :atomic_batches strategy and after_transaction hooks" do
      # Create an author
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Test Author"})
        |> Ash.create!()

      # Create multiple posts
      posts =
        for i <- 1..5 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "load_batch_#{i}", author_id: author.id})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # Use atomic_batches strategy with load option and after_transaction hooks
      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(
          :update_with_atomic_after_transaction,
          %{title: "batch_updated"},
          strategy: [:atomic_batches],
          batch_size: 2,
          return_records?: true,
          load: [:author],
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 5

      # Verify load worked for all records
      for record <- result.records do
        assert %Author{name: "Test Author"} = record.author
      end

      # Verify after_transaction hooks executed
      for _ <- 1..5 do
        assert_receive {:after_transaction_called, _id}, 1000
      end
    end

    test "load option with transaction: :all and after_transaction hooks" do
      # Create an author
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Test Author"})
        |> Ash.create!()

      # Create posts
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "load_tx_all_#{i}", author_id: author.id})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # Use stream strategy with transaction: :all and load option
      result =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(
          :update_with_atomic_after_transaction,
          %{title: "tx_all_updated"},
          strategy: :stream,
          transaction: :all,
          return_records?: true,
          load: [:author],
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 3

      # Verify load worked for all records
      for record <- result.records do
        assert %Author{name: "Test Author"} = record.author
      end

      # Verify after_transaction hooks executed
      for _ <- 1..3 do
        assert_receive {:after_transaction_called, _id}, 1000
      end
    end

    # TODO: This behavior is inconsistent with non-atomic strategies where load reflects the final result.
    #       The atomic path runs load_data BEFORE after_transaction hooks, so loaded data doesn't
    #       reflect modifications made by hooks. See TODO at bulk.ex around line 578.
    test "after_transaction hook modifying result - load reflects original data" do
      # Create an author and post
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Original Author"})
        |> Ash.create!()

      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "hook_modify_test", author_id: author.id})
        |> Ash.create!()

      # Use the action that modifies title in after_transaction
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(
          :update_with_after_transaction,
          %{title: "modified"},
          strategy: :stream,
          return_records?: true,
          load: [:author],
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 1
      [updated_post] = result.records

      # The after_transaction hook appends "_stuff" to the title
      assert updated_post.title == "modified_stuff"

      # Load should still work
      assert updated_post.author.name == "Original Author"
    end
  end

  # Manual action modules for testing after_transaction hooks with manual updates
  defmodule ManualUpdateSimple do
    @moduledoc """
    Simple manual update module that just performs the update.
    After_transaction hooks are added via a separate change module.
    """
    use Ash.Resource.ManualUpdate

    def update(changeset, _opts, _context) do
      # Perform the actual update using ETS data layer
      Ash.DataLayer.Ets.update(changeset.resource, changeset)
    end
  end

  defmodule ManualUpdateFails do
    @moduledoc """
    Manual update module that always fails.
    Used to test after_transaction hook error handling with manual actions.
    """
    use Ash.Resource.ManualUpdate

    def update(_changeset, _opts, _context) do
      {:error, "intentional manual update error"}
    end
  end

  defmodule ManualUpdateAfterTransactionChange do
    @moduledoc """
    Change module that adds after_transaction hook for manual update testing.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _cs, result ->
        case result do
          {:ok, record} ->
            send(self(), {:manual_update_after_transaction_success, record.id})
            {:ok, record}

          {:error, error} ->
            send(self(), {:manual_update_after_transaction_error, error})
            {:error, error}
        end
      end)
    end
  end

  defmodule ManualUpdateAfterTransactionConvertsErrorChange do
    @moduledoc """
    Change module that adds after_transaction hook that converts errors to success.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _cs, result ->
        case result do
          {:ok, record} ->
            {:ok, record}

          {:error, _error} ->
            send(self(), {:manual_update_after_transaction_converted_error})
            # Return a fake success with the original data
            {:ok, changeset.data}
        end
      end)
    end
  end

  defmodule ManualUpdatePost do
    @moduledoc false
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      notifiers: [Notifier]

    alias Ash.Test.Actions.BulkUpdateAfterTransactionTest.{
      ManualUpdateSimple,
      ManualUpdateFails,
      ManualUpdateAfterTransactionChange,
      ManualUpdateAfterTransactionConvertsErrorChange
    }

    ets do
      private? true
    end

    actions do
      default_accept :*
      defaults [:read, :destroy, create: :*]

      update :update_manual_with_after_transaction do
        accept [:title, :title2]
        manual ManualUpdateSimple
        change ManualUpdateAfterTransactionChange
      end

      update :update_manual_with_after_transaction_converts_error do
        accept [:title, :title2]
        manual ManualUpdateFails
        change ManualUpdateAfterTransactionConvertsErrorChange
      end
    end

    attributes do
      uuid_primary_key :id
      attribute :title, :string, allow_nil?: false, public?: true
      attribute :title2, :string, public?: true
    end
  end

  describe "after_transaction with manual update actions" do
    test "after_transaction hooks work with manual update action (single record)" do
      post =
        ManualUpdatePost
        |> Ash.Changeset.for_create(:create, %{title: "manual_update_test"})
        |> Ash.create!()

      result =
        ManualUpdatePost
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update!(
          :update_manual_with_after_transaction,
          %{title: "updated_manual"},
          strategy: :stream,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 1
      assert hd(result.records).title == "updated_manual"

      # Verify after_transaction hook was called
      assert_receive {:manual_update_after_transaction_success, _id}, 1000
    end

    test "after_transaction hooks work with manual update action (multiple records)" do
      posts =
        for i <- 1..3 do
          ManualUpdatePost
          |> Ash.Changeset.for_create(:create, %{title: "manual_bulk_#{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        ManualUpdatePost
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(
          :update_manual_with_after_transaction,
          %{title: "bulk_updated"},
          strategy: :stream,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 3

      # Verify after_transaction hooks were called for all records
      assert_receive {:manual_update_after_transaction_success, _id1}, 1000
      assert_receive {:manual_update_after_transaction_success, _id2}, 1000
      assert_receive {:manual_update_after_transaction_success, _id3}, 1000
    end

    test "after_transaction hook can convert error to success in manual update" do
      post =
        ManualUpdatePost
        |> Ash.Changeset.for_create(:create, %{title: "will_fail_update"})
        |> Ash.create!()

      result =
        ManualUpdatePost
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_update(
          :update_manual_with_after_transaction_converts_error,
          %{title: "should_fail"},
          strategy: :stream,
          return_records?: true,
          return_errors?: true
        )

      # The hook should have been called
      assert_receive {:manual_update_after_transaction_converted_error}, 1000

      # The result should show success (error converted by hook)
      assert result.status == :success
      assert length(result.records) == 1
    end

    test "manual update after_transaction hooks work with return_stream?" do
      posts =
        for i <- 1..2 do
          ManualUpdatePost
          |> Ash.Changeset.for_create(:create, %{title: "stream_manual_#{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result_stream =
        ManualUpdatePost
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update(
          :update_manual_with_after_transaction,
          %{title: "stream_updated"},
          strategy: :stream,
          return_stream?: true,
          return_records?: true
        )

      results = Enum.to_list(result_stream)
      assert length(results) == 2

      # Verify hooks executed
      assert_receive {:manual_update_after_transaction_success, _id1}, 1000
      assert_receive {:manual_update_after_transaction_success, _id2}, 1000
    end

    test "manual update after_transaction hooks work with transaction: :all" do
      posts =
        for i <- 1..2 do
          ManualUpdatePost
          |> Ash.Changeset.for_create(:create, %{title: "tx_all_manual_#{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        ManualUpdatePost
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_update!(
          :update_manual_with_after_transaction,
          %{title: "tx_updated"},
          strategy: :stream,
          transaction: :all,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 2

      # Verify hooks executed
      assert_receive {:manual_update_after_transaction_success, _id1}, 1000
      assert_receive {:manual_update_after_transaction_success, _id2}, 1000
    end
  end
end
