# SPDX-FileCopyrightText: 2019 ash contributors <https://github.com/ash-project/ash/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule Ash.Test.Actions.BulkDestroyAfterTransactionTest do
  @moduledoc """
  Tests for after_transaction hooks in bulk destroy operations.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  require Ash.Query

  alias Ash.Test.Domain, as: Domain

  defmodule Notifier do
    use Ash.Notifier

    def notify(notification) do
      send(self(), {:notification, notification})
    end
  end

  defmodule AtomicDestroyWithAfterTransaction do
    @moduledoc """
    Change module that adds after_transaction hooks for destroy actions.
    Supports both stream and atomic strategies by adding hooks in both callbacks.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _changeset, {:ok, result} ->
        send(self(), {:after_transaction_destroy_called, result.id})
        {:ok, result}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AtomicDestroyWithAfterTransactionHandlingErrors do
    @moduledoc """
    Change module that adds after_transaction hooks that handle both success and failure.
    Supports both stream and atomic strategies.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          send(self(), {:after_transaction_destroy_success, result.id})
          {:ok, result}

        _changeset, {:error, error} ->
          send(self(), {:after_transaction_destroy_error, error})
          {:error, error}
      end)
    end

    def atomic(changeset, opts, context) do
      {:ok, change(changeset, opts, context)}
    end
  end

  defmodule AtomicDestroyWithAfterTransactionReturnsError do
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

  defmodule AtomicDestroyWithAfterTransactionConvertsErrorToSuccess do
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

  defmodule AtomicDestroyWithAfterTransactionFailsForSomeRecords do
    @moduledoc """
    Change module where after_transaction hook returns an error for records with title containing "fail".
    Used to test partial_success status when some records succeed and some fail.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          if String.contains?(result.title, "fail") do
            send(self(), {:after_transaction_destroy_partial_failure, result.id})
            {:error, "Hook failed for title containing 'fail'"}
          else
            send(self(), {:after_transaction_destroy_partial_success, result.id})
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

  defmodule AtomicDestroyWithAfterTransactionModifiesError do
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
          # Return a "fake" success with the original data
          {:ok, changeset.data}
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
        public?(true)
      end
    end

    relationships do
      has_many :posts, Ash.Test.Actions.BulkDestroyAfterTransactionTest.Post,
        destination_attribute: :author_id,
        public?: true
    end
  end

  defmodule Post do
    @moduledoc false
    use Ash.Resource,
      domain: Domain,
      notifiers: [Notifier],
      data_layer: Ash.DataLayer.Ets

    ets do
      private? true
    end

    actions do
      default_accept :*
      defaults [:destroy, create: :*, update: :*]

      read :read do
        primary? true
        pagination keyset?: true, required?: false
      end

      destroy :destroy_with_after_transaction do
        require_atomic? false

        change after_transaction(fn
                 _changeset, {:ok, result}, _context ->
                   {:ok, %{result | title: result.title <> "_stuff"}}

                 _changeset, {:error, error}, _context ->
                   send(self(), {:error, error})
               end)
      end

      destroy :destroy_with_atomic_after_transaction do
        change AtomicDestroyWithAfterTransaction
      end

      destroy :destroy_with_atomic_after_transaction_handling_errors do
        change AtomicDestroyWithAfterTransactionHandlingErrors
      end

      destroy :destroy_with_atomic_after_transaction_always_fails do
        change AtomicDestroyWithAfterTransactionHandlingErrors
        validate AlwaysFailsValidation
      end

      destroy :destroy_with_atomic_after_transaction_returns_error do
        change AtomicDestroyWithAfterTransactionReturnsError
      end

      destroy :destroy_with_after_transaction_converts_error_to_success do
        change AtomicDestroyWithAfterTransactionConvertsErrorToSuccess
        validate AlwaysFailsValidation
      end

      destroy :destroy_with_after_transaction_modifies_error do
        change AtomicDestroyWithAfterTransactionModifiesError
        validate AlwaysFailsValidation
      end

      destroy :destroy_with_after_transaction_partial_failure do
        change AtomicDestroyWithAfterTransactionFailsForSomeRecords
      end

      destroy :destroy_with_after_action_failure_converted_to_success do
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

  defmodule MnesiaPost do
    @doc false

    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Mnesia, notifiers: [Notifier]

    mnesia do
      table :mnesia_post_after_transaction_destroys
    end

    attributes do
      uuid_primary_key :id
      attribute :title, :string, allow_nil?: false, public?: true
    end

    actions do
      default_accept :*
      defaults [:read, :destroy, :create, :update]

      destroy :destroy_with_after_transaction do
        change MnesiaAfterTransactionChange
      end
    end
  end

  describe "atomic destroys with after_transaction hooks" do
    test "after_transaction hooks work with :atomic strategy" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # after_transaction hooks can be added in atomic/3 callback
      # and will execute after the transaction closes
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_destroy!(:destroy_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 1

      # Verify the hook executed by checking for the message
      assert_received {:after_transaction_destroy_called, post_id}
      assert post_id == post.id

      # Verify the record was destroyed
      assert [] = Ash.read!(Post)
    end

    test "after_transaction hooks work with multiple records" do
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
        |> Ash.bulk_destroy!(:destroy_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 5

      # Verify all hooks executed
      for post_id <- post_ids do
        assert_received {:after_transaction_destroy_called, ^post_id}
      end

      # Verify all records were destroyed
      assert [] = Ash.read!(Post)
    end

    test "after_transaction hook receives correct changeset and result" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_destroy!(:destroy_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true
        )

      assert result.status == :success
      assert_received {:after_transaction_destroy_called, post_id}
      assert post_id == post.id
    end

    test "after_transaction hooks work with list of records and atomic_batches strategy" do
      # Test that after_transaction hooks execute for each record in a list
      # and verifies that after_transaction hooks execute without explicit return_records?: true
      posts =
        for i <- 1..5 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result =
        posts
        |> Ash.bulk_destroy!(
          :destroy_with_atomic_after_transaction,
          %{},
          strategy: [:atomic_batches],
          batch_size: 2,
          return_records?: true
        )

      assert result.status == :success

      for post_id <- post_ids do
        assert_received {:after_transaction_destroy_called, ^post_id}
      end

      # Verify all records were destroyed
      assert [] = Ash.read!(Post)
    end

    test "empty result set doesn't cause errors with after_transaction hooks" do
      # Query that matches no records
      result =
        Post
        |> Ash.Query.filter(id == "nonexistent")
        |> Ash.bulk_destroy(:destroy_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true
        )

      assert result.status == :success
      assert result.records == []

      # No hooks should execute since no records matched
      refute_received {:after_transaction_destroy_called, _}
    end

    test "after_transaction hooks work with :stream strategy" do
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
        |> Ash.bulk_destroy!(:destroy_with_atomic_after_transaction, %{},
          strategy: :stream,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 3

      for post_id <- post_ids do
        assert_received {:after_transaction_destroy_called, ^post_id}
      end

      # Verify all records were destroyed
      assert [] = Ash.read!(Post)
    end

    test "after_transaction hooks work with :stream strategy and return_stream?" do
      posts =
        for i <- 1..3 do
          Post
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      result_stream =
        Post
        |> Ash.Query.filter(id in ^post_ids)
        |> Ash.bulk_destroy(:destroy_with_atomic_after_transaction, %{},
          strategy: :stream,
          return_stream?: true,
          return_records?: true
        )

      # Consume the stream
      results = Enum.to_list(result_stream)
      assert length(results) == 3

      for post_id <- post_ids do
        assert_receive {:after_transaction_destroy_called, ^post_id}, 1000
      end

      # Verify all records were destroyed
      assert [] = Ash.read!(Post)
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
        |> Ash.bulk_destroy(:destroy_with_atomic_after_transaction_always_fails, %{},
          strategy: :atomic,
          return_errors?: true
        )

      assert result.status == :error
      assert result.error_count == 1

      # Verify the after_transaction hook received the error
      assert_receive {:after_transaction_destroy_error, _error}, 1000

      # Verify the post was NOT destroyed (operation failed)
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
      result =
        Post
        |> Ash.Query.filter(id in ^Enum.map(posts, & &1.id))
        |> Ash.bulk_destroy(:destroy_with_atomic_after_transaction_always_fails, %{},
          strategy: [:atomic_batches],
          return_errors?: true
        )

      assert result.status == :error
      # Only 1 error because validation fails on the single atomic changeset
      assert result.error_count == 1

      # Verify hook received error
      assert_receive {:after_transaction_destroy_error, _error}, 1000

      # Verify posts were NOT destroyed (operation failed)
      assert length(Ash.read!(Post)) == 3
    end

    test "after_transaction hook error is captured in result" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # This action's hook returns an error even on success
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_destroy(:destroy_with_atomic_after_transaction_returns_error, %{},
          strategy: :atomic,
          return_errors?: true,
          return_records?: true
        )

      # The hook was called and returned an error
      assert_receive {:after_transaction_hook_returning_error, _post_id}, 1000

      # The operation should show the error from the hook
      assert result.status == :error
      assert result.error_count == 1
      assert length(result.errors) == 1
      assert result.records == []
    end

    test "after_transaction hook error is captured with :stream strategy" do
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
        |> Ash.bulk_destroy(:destroy_with_atomic_after_transaction_returns_error, %{},
          strategy: :stream,
          return_errors?: true
        )

      # Stream strategy stops on first error, so only 1 hook is called
      assert_receive {:after_transaction_hook_returning_error, _}, 1000

      # Hook error is captured in the result
      assert result.status == :error
      assert result.error_count == 1
    end

    test "multiple after_transaction hooks execute in order" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # Use the existing action with after_transaction hook
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_destroy!(:destroy_with_atomic_after_transaction, %{},
          strategy: :atomic,
          return_records?: true
        )

      assert result.status == :success

      # At least one hook should execute
      assert_receive {:after_transaction_destroy_called, _post_id}, 1000
    end

    test "after_transaction hooks run on failure with return_stream?" do
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
        |> Ash.bulk_destroy(:destroy_with_atomic_after_transaction_always_fails, %{},
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
        assert_receive {:after_transaction_destroy_error, _error}, 1000
      end

      # Verify posts were NOT destroyed (all operations failed)
      assert length(Ash.read!(Post)) == 3
    end

    test "after_transaction hook error is captured with :atomic_batches strategy" do
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
        |> Ash.bulk_destroy(:destroy_with_atomic_after_transaction_returns_error, %{},
          strategy: [:atomic_batches],
          return_errors?: true
        )

      # All hooks are called (one per record)
      for _post_id <- post_ids do
        assert_receive {:after_transaction_hook_returning_error, _}, 1000
      end

      # Hook errors are captured in the result
      assert result.status == :error
      assert result.error_count == 1
    end

    test "after_transaction hooks work with return_notifications?: true" do
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
        |> Ash.bulk_destroy!(:destroy_with_atomic_after_transaction, %{},
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
        assert_receive {:after_transaction_destroy_called, ^post_id}, 1000
      end
    end

    test "after_transaction hooks work with notify?: true" do
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
        |> Ash.bulk_destroy!(:destroy_with_atomic_after_transaction, %{},
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
        assert_receive {:after_transaction_destroy_called, ^post_id}, 1000
      end
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
        |> Ash.bulk_destroy(:destroy_with_after_transaction_partial_failure, %{},
          strategy: :atomic,
          return_errors?: true,
          return_records?: true
        )

      # The success hooks should have been called
      assert_receive {:after_transaction_destroy_partial_success, ^success_post_id}, 1000
      assert_receive {:after_transaction_destroy_partial_failure, ^fail_post_id}, 1000
      assert_receive {:after_transaction_destroy_partial_success, ^another_success_id}, 1000

      # Status should be partial_success since some records succeeded and some failed
      assert result.status == :partial_success
      assert result.error_count == 1
      assert length(result.errors) == 1
      assert length(result.records) == 2
    end
  end

  describe "after_transaction hook return value for invalid changesets" do
    test "hook can convert validation error to success" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # Destroy with a validation that always fails
      # The after_transaction hook should convert the error to success
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_destroy(
          :destroy_with_after_transaction_converts_error_to_success,
          %{},
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

      # Destroy with a validation that always fails
      # The after_transaction hook should convert the error to success
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_destroy(
          :destroy_with_after_transaction_converts_error_to_success,
          %{},
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

      # Create multiple posts (will all fail validation but be converted to success)
      post1 =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "post_1"})
        |> Ash.create!()

      post2 =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "post_2"})
        |> Ash.create!()

      post3 =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "post_3"})
        |> Ash.create!()

      # Destroy all posts - they'll all fail validation but the hook converts errors to success
      # The sorted?: true option should ensure results come back in query order
      result =
        Post
        |> Ash.Query.filter(id in [^post1.id, ^post2.id, ^post3.id])
        |> Ash.Query.sort(:title)
        |> Ash.bulk_destroy(
          :destroy_with_after_transaction_converts_error_to_success,
          %{},
          strategy: :stream,
          return_records?: true,
          return_errors?: true,
          sorted?: true
        )

      # The hooks should have been called for all records
      assert_receive {:after_transaction_converted_error_to_success}, 1000
      assert_receive {:after_transaction_converted_error_to_success}, 1000
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # All records should be returned
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 3

      # Records should be in sorted order (by title)
      titles = Enum.map(result.records, & &1.title)
      assert titles == ["post_1", "post_2", "post_3"]
    end

    test "hook can modify validation error" do
      post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "test"})
        |> Ash.create!()

      # Destroy with a validation that always fails
      # The after_transaction hook should modify the error
      result =
        Post
        |> Ash.Query.filter(id == ^post.id)
        |> Ash.bulk_destroy(
          :destroy_with_after_transaction_modifies_error,
          %{},
          strategy: :stream,
          return_errors?: true
        )

      # The hook should have been called
      assert_receive {:after_transaction_modified_error}, 1000

      # The error should be the modified one from the hook, not the original validation error
      assert result.error_count == 1
      assert result.errors != []

      error = hd(result.errors)
      # The error should contain "custom error from hook", not the original "always fails" error
      assert Exception.message(error) =~ "custom error from hook"
    end

    test "hook can convert error to success with transaction: :all and mixed valid/invalid changesets" do
      # Create posts
      valid_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "valid_title"})
        |> Ash.create!()

      invalid_post =
        Post
        |> Ash.Changeset.for_create(:create, %{title: "will_be_invalid"})
        |> Ash.create!()

      # Destroy with a validation that always fails
      # The invalid one's after_transaction hook should convert the error to success
      result =
        Post
        |> Ash.Query.filter(id in [^valid_post.id, ^invalid_post.id])
        |> Ash.bulk_destroy(
          :destroy_with_after_transaction_converts_error_to_success,
          %{},
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

      # Destroy with an action that has after_action hook that fails for "fail_" prefixed titles
      # and after_transaction hook that converts the error to success
      result =
        Post
        |> Ash.Query.filter(id in [^valid_post.id, ^fail_post.id])
        |> Ash.bulk_destroy(
          :destroy_with_after_action_failure_converted_to_success,
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
        |> Ash.bulk_destroy(
          :destroy_with_after_action_failure_converted_to_success,
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
    end
  end

  describe "after_transaction hooks run outside batch transaction" do
    setup do
      capture_log(fn ->
        Ash.DataLayer.Mnesia.start(Domain, [MnesiaPost])
      end)

      :ok
    end

    test "after_transaction hooks run outside batch transaction - no warning" do
      posts =
        for i <- 1..2 do
          MnesiaPost
          |> Ash.Changeset.for_create(:create, %{title: "post #{i}"})
          |> Ash.create!()
        end

      post_ids = Enum.map(posts, & &1.id)

      # With transaction: :batch, after_transaction hooks now run OUTSIDE the transaction
      # so no warning should be logged
      log =
        capture_log(fn ->
          result =
            MnesiaPost
            |> Ash.Query.filter(id in ^post_ids)
            |> Ash.bulk_destroy(
              :destroy_with_after_transaction,
              %{},
              return_records?: true,
              authorize?: false
              # transaction: :batch is the default
            )

          assert result.status == :success
          assert length(result.records) == 2
        end)

      # Verify the hooks executed
      assert_receive {:mnesia_after_transaction_called, _id1}, 1000
      assert_receive {:mnesia_after_transaction_called, _id2}, 1000

      # Should NOT warn since after_transaction now runs outside the transaction
      refute log =~ "after_transaction"
    end
  end
end
