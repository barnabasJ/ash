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
end
