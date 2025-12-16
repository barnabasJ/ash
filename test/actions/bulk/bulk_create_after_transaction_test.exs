# SPDX-FileCopyrightText: 2019 ash contributors <https://github.com/ash-project/ash/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule Ash.Test.Actions.BulkCreateAfterTransactionTest do
  @moduledoc """
  Tests for after_transaction hooks in bulk create operations.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Ash.Test.Domain, as: Domain

  defmodule Notifier do
    use Ash.Notifier

    def notify(notification) do
      send(self(), {:notification, notification})
    end
  end

  defmodule AfterTransactionChange do
    @moduledoc """
    Change module that adds after_transaction hooks for create actions.
    Sends a message to verify the hook executed.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _changeset, {:ok, result} ->
        send(self(), {:after_transaction_create_called, result.id})
        {:ok, result}
      end)
    end
  end

  defmodule AfterTransactionReturnsError do
    @moduledoc """
    Change module where after_transaction hook returns an error on success.
    Used to test that hook errors are captured properly.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn
        _changeset, {:ok, result} ->
          send(self(), {:after_transaction_create_returning_error, result.id})
          {:error, "Hook intentionally returned error"}

        _changeset, {:error, error} ->
          {:error, error}
      end)
    end
  end

  defmodule MultipleAfterTransactionHooks do
    @moduledoc """
    Change module that adds multiple after_transaction hooks.
    Used to test execution order.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      changeset
      |> Ash.Changeset.after_transaction(fn _changeset, {:ok, result} ->
        send(self(), {:hook_1_executed, result.id})
        {:ok, result}
      end)
      |> Ash.Changeset.after_transaction(fn _changeset, {:ok, result} ->
        send(self(), {:hook_2_executed, result.id})
        {:ok, result}
      end)
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
          # Return a "fake" success with a default record
          {:ok,
           %{
             changeset.data
             | id: Ash.UUID.generate(),
               title: "recovered_from_after_action_failure"
           }}
      end)
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

  defmodule Org do
    @moduledoc false
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets

    ets do
      private?(true)
    end

    attributes do
      uuid_primary_key :id
    end

    actions do
      default_accept :*
      defaults create: :*
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
      has_many :posts, Ash.Test.Actions.BulkCreateAfterTransactionTest.Post,
        destination_attribute: :author_id,
        public?: true
    end
  end

  defmodule Post do
    @moduledoc false
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      notifiers: [Notifier]

    alias Ash.Test.Actions.BulkCreateAfterTransactionTest.Org

    ets do
      private? true
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
      global? true
    end

    actions do
      default_accept :*
      defaults [:read, :destroy, create: :*, update: :*]

      create :create_with_after_transaction do
        change after_transaction(fn
                 _changeset, {:ok, result}, _context ->
                   {:ok, %{result | title: result.title <> "_stuff"}}

                 _changeset, {:error, error}, _context ->
                   send(self(), {:error, error})
                   {:error, error}
               end)
      end

      create :create_with_after_transaction_hook do
        change AfterTransactionChange
      end

      create :create_with_after_transaction_returns_error do
        change AfterTransactionReturnsError
      end

      create :create_with_after_transaction_converts_error_to_success do
        change after_transaction(fn
                 _changeset, {:ok, result}, _context ->
                   {:ok, result}

                 changeset, {:error, _original_error}, _context ->
                   send(self(), {:after_transaction_converted_error_to_success})
                   # Return a "fake" success with a default record
                   {:ok, %{changeset.data | id: Ash.UUID.generate(), title: "default_from_hook"}}
               end)
      end

      create :create_with_after_transaction_converts_error_with_author do
        argument :author_id, :uuid

        change after_transaction(fn
                 _changeset, {:ok, result}, _context ->
                   {:ok, result}

                 changeset, {:error, _original_error}, _context ->
                   send(self(), {:after_transaction_converted_error_with_author})
                   # Return a "fake" success with a default record including author_id
                   author_id = Ash.Changeset.get_argument(changeset, :author_id)

                   {:ok,
                    %{
                      changeset.data
                      | id: Ash.UUID.generate(),
                        title: "default_from_hook",
                        author_id: author_id
                    }}
               end)
      end

      create :create_with_after_transaction_modifies_error do
        change after_transaction(fn
                 _changeset, {:ok, result}, _context ->
                   {:ok, result}

                 _changeset, {:error, _original_error}, _context ->
                   send(self(), {:after_transaction_modified_error})
                   {:error, "custom error from hook"}
               end)
      end

      create :create_with_multiple_hooks do
        change MultipleAfterTransactionHooks
      end

      create :create_with_after_action_failure_converted_to_success do
        change AfterActionFailsWithAfterTransaction
      end
    end

    attributes do
      uuid_primary_key :id

      attribute :title, :string do
        public?(true)
        allow_nil?(false)
      end

      attribute :title2, :string do
        public?(true)
      end

      attribute :org_id, :uuid do
        public?(true)
      end
    end

    relationships do
      belongs_to :org, Org, public?: true, attribute_writable?: true
      belongs_to :author, Author, public?: true
    end
  end

  defmodule MnesiaPost do
    @moduledoc false
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Mnesia

    mnesia do
      table :mnesia_post_after_transaction_creates
    end

    attributes do
      uuid_primary_key :id
      attribute :title, :string, allow_nil?: false, public?: true
    end

    actions do
      default_accept :*
      defaults [:read, :destroy, :create]

      create :create_with_after_action_error_and_after_transaction do
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

      create :create_with_after_transaction do
        change MnesiaAfterTransactionChange
      end
    end
  end

  describe "after_transaction hooks with return_stream?" do
    test "hooks work with return_stream?" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # return_stream?: true streams results back
      result_stream =
        Ash.bulk_create(
          [%{title: "title1"}, %{title: "title2"}, %{title: "title3"}],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          return_stream?: true,
          return_records?: true,
          authorize?: false
        )

      # Consume the stream
      results = Enum.to_list(result_stream)
      assert length(results) == 3

      # Verify hooks executed (use assert_receive with timeout for async operations)
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000
      assert_receive {:after_transaction_create_called, _id3}, 1000
    end

    test "hooks work with return_stream? on failure" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create with invalid data to trigger failure
      result_stream =
        Ash.bulk_create(
          [%{title: 1}, %{title: 2}],
          Post,
          :create_with_after_transaction,
          tenant: org.id,
          return_stream?: true,
          return_errors?: true,
          authorize?: false
        )

      # Consume the stream
      results = Enum.to_list(result_stream)

      # Each record should fail
      for result <- results do
        assert {:error, _} = result
      end

      # Verify hooks executed for each failed record
      assert_receive {:error, _error}, 1000
      assert_receive {:error, _error}, 1000
    end
  end

  describe "after_transaction hooks basic functionality" do
    test "hooks handle empty result set" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Empty input - no records to create
      result =
        Ash.bulk_create(
          [],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          return_records?: true,
          authorize?: false
        )

      assert result.status == :success
      assert result.records == []
      # No hooks should execute since no records were created
      refute_receive {:after_transaction_create_called, _}
    end

    test "hook error is captured in result" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # This action's hook returns an error even on success
      result =
        Ash.bulk_create(
          [%{title: "test"}],
          Post,
          :create_with_after_transaction_returns_error,
          tenant: org.id,
          return_records?: true,
          return_errors?: true,
          authorize?: false
        )

      # The hook was called and returned an error
      assert_receive {:after_transaction_create_returning_error, _id}, 1000

      # The operation should show the error from the hook
      assert result.error_count == 1
    end

    test "multiple hooks execute in order" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [%{title: "test"}],
          Post,
          :create_with_multiple_hooks,
          tenant: org.id,
          return_records?: true,
          authorize?: false
        )

      assert result.status == :success

      # Verify hooks executed in order (hook_1 before hook_2)
      assert_receive {:hook_1_executed, id}, 1000
      assert_receive {:hook_2_executed, ^id}, 1000
    end

    test "hooks work with return_notifications?: true" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [%{title: "test1"}, %{title: "test2"}],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          return_records?: true,
          return_notifications?: true,
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 2
      # Notifications should be returned
      assert length(result.notifications) == 2

      # Verify hooks executed
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000
    end

    test "hooks work with notify?: true" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [%{title: "test1"}, %{title: "test2"}],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          return_records?: true,
          notify?: true,
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 2

      # Notifications should be sent (via Notifier module)
      assert_received {:notification, %{data: %{title: "test1"}}}
      assert_received {:notification, %{data: %{title: "test2"}}}

      # Verify after_transaction hooks also executed
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000
    end

    test "with explicit return_errors?: true returns errors list" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # This action's hook returns an error even on success
      # Note: return_records?: true is needed for after_transaction hooks to execute
      # With transaction: :batch (default), the first error stops the batch
      result =
        Ash.bulk_create(
          [%{title: "test1"}],
          Post,
          :create_with_after_transaction_returns_error,
          tenant: org.id,
          return_records?: true,
          return_errors?: true,
          authorize?: false
        )

      # Hook executed and returned error
      assert_receive {:after_transaction_create_returning_error, _id1}, 1000

      # Error should be captured
      assert result.status == :error
      assert result.error_count == 1
      assert length(result.errors) == 1
    end

    test "with both return_records?: true and return_errors?: true" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create(
          [%{title: "test1"}, %{title: "test2"}],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          return_records?: true,
          return_errors?: true,
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 2
      assert result.errors == []

      # Verify hooks executed
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000
    end

    test "hooks execute with default return options require return_records?: true" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Call without return_records? - after_transaction hooks don't execute
      # because records are not processed through the full result pipeline
      result =
        Ash.bulk_create(
          [%{title: "test1"}, %{title: "test2"}],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          authorize?: false
        )

      assert result.status == :success

      # Without return_records?: true, hooks don't execute
      refute_receive {:after_transaction_create_called, _}

      # Records are still created in the database
      assert length(Ash.read!(Post, tenant: org.id)) == 2
    end
  end

  describe "after_transaction hooks with transaction strategies" do
    test "hooks work with transaction: :batch (default) on success" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [%{title: "test1"}, %{title: "test2"}, %{title: "test3"}],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          return_records?: true,
          authorize?: false
          # transaction: :batch is the default
        )

      assert result.status == :success
      assert length(result.records) == 3

      # Verify hooks executed for all records
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000
      assert_receive {:after_transaction_create_called, _id3}, 1000
    end

    test "hooks work with transaction: :all on success" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [%{title: "test1"}, %{title: "test2"}, %{title: "test3"}],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          transaction: :all,
          return_records?: true,
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 3

      # Verify hooks executed for all records
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000
      assert_receive {:after_transaction_create_called, _id3}, 1000
    end

    test "hooks work with return_stream? and transaction: :batch" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result_stream =
        Ash.bulk_create(
          [%{title: "test1"}, %{title: "test2"}],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          return_stream?: true,
          return_records?: true,
          authorize?: false
        )

      # Consume the stream
      results = Enum.to_list(result_stream)
      assert length(results) == 2

      # Verify hooks executed
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000
    end
  end

  describe "after_transaction hook return value for invalid changesets" do
    test "hook can convert validation error to success" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create with invalid data (title is nil, but required)
      # The after_transaction hook should convert the error to success
      result =
        Ash.bulk_create(
          [%{title: nil}],
          Post,
          :create_with_after_transaction_converts_error_to_success,
          tenant: org.id,
          return_records?: true,
          return_errors?: true,
          authorize?: false
        )

      # The hook should have been called
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # The result should show success, not error
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 1
      assert hd(result.records).title == "default_from_hook"
    end

    test "hook can modify validation error" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create with invalid data (title is nil, but required)
      # The after_transaction hook should modify the error
      result =
        Ash.bulk_create(
          [%{title: nil}],
          Post,
          :create_with_after_transaction_modifies_error,
          tenant: org.id,
          return_errors?: true,
          authorize?: false
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
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create with a mix of valid and invalid data
      # The invalid one's after_transaction hook should convert the error to success
      # This tests the case where hook_success_results need to be properly handled
      # when there are also valid changesets in the batch
      result =
        Ash.bulk_create(
          [%{title: "valid_title"}, %{title: nil}],
          Post,
          :create_with_after_transaction_converts_error_to_success,
          tenant: org.id,
          transaction: :all,
          return_records?: true,
          return_errors?: true,
          authorize?: false,
          sorted?: true
        )

      # The hook should have been called for the invalid changeset
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # Both records should be returned - the valid one created normally,
      # and the invalid one converted to success by the hook
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 2

      # Verify both records are present
      titles = Enum.map(result.records, & &1.title)
      assert "valid_title" in titles
      assert "default_from_hook" in titles
    end

    test "hook can convert error to success with transaction: :all and mixed valid/invalid changesets (unsorted)" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Same test without sorted? to expose deeper issues with hook_success_results
      # not being properly indexed for run_after_action_hooks
      result =
        Ash.bulk_create(
          [%{title: "valid_title"}, %{title: nil}],
          Post,
          :create_with_after_transaction_converts_error_to_success,
          tenant: org.id,
          transaction: :all,
          return_records?: true,
          return_errors?: true,
          authorize?: false
        )

      # The hook should have been called for the invalid changeset
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # Both records should be returned - the valid one created normally,
      # and the invalid one converted to success by the hook
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 2

      # Verify both records are present
      titles = Enum.map(result.records, & &1.title)
      assert "valid_title" in titles
      assert "default_from_hook" in titles
    end

    test "after_action failure converted to success in run_batch with mixed valid/invalid" do
      # This test exposes the bug where hook_success_results from after_action failures
      # that are converted to success by after_transaction hooks are not properly handled
      # when appended at line 1405 in run_batch. The changeset is not in changesets_by_ref
      # because the indexes were rebuilt from valid changesets only.
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create with a mix of valid and "fail_" prefixed titles
      # The fail_ record will pass initial validation but fail in after_action,
      # then be recovered by after_transaction
      result =
        Ash.bulk_create(
          [%{title: "valid_title"}, %{title: "fail_this_one"}],
          Post,
          :create_with_after_action_failure_converted_to_success,
          tenant: org.id,
          transaction: :all,
          return_records?: true,
          return_errors?: true,
          authorize?: false
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
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create(
          [%{title: "valid_title"}, %{title: "fail_this_one"}],
          Post,
          :create_with_after_action_failure_converted_to_success,
          tenant: org.id,
          return_records?: true,
          return_errors?: true,
          authorize?: false
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
      # This test exposes a bug where records returned from after_transaction hooks
      # that convert errors to success don't get the load option applied.
      # The issue is that these records are added to must_be_simple_results which
      # is concatenated AFTER process_results, but the loading happens INSIDE
      # process_results.
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create an author to reference
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Test Author"})
        |> Ash.create!()

      # Create with invalid data (title is nil, but required)
      # The after_transaction hook should convert the error to success
      # and return a record with the author_id set
      result =
        Ash.bulk_create(
          [%{title: nil, author_id: author.id}],
          Post,
          :create_with_after_transaction_converts_error_with_author,
          tenant: org.id,
          return_records?: true,
          return_errors?: true,
          authorize?: false,
          load: [:author]
        )

      # The hook should have been called
      assert_receive {:after_transaction_converted_error_with_author}, 1000

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
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create with a mix: valid at index 0, invalid at index 1, valid at index 2
      # The invalid one will be converted to success by after_transaction
      result =
        Ash.bulk_create(
          [
            %{title: "first_valid"},
            %{title: nil},
            %{title: "third_valid"}
          ],
          Post,
          :create_with_after_transaction_converts_error_to_success,
          tenant: org.id,
          return_records?: true,
          return_errors?: true,
          authorize?: false,
          sorted?: true
        )

      # The hook should have been called for the invalid changeset
      assert_receive {:after_transaction_converted_error_to_success}, 1000

      # All three should succeed
      assert result.status == :success
      assert result.error_count == 0
      assert length(result.records) == 3

      # With sorted?: true, records should be in original input order
      titles = Enum.map(result.records, & &1.title)
      assert titles == ["first_valid", "default_from_hook", "third_valid"]
    end
  end

  describe "after_transaction hooks run outside batch transaction" do
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
      result =
        [%{title: "title1"}, %{title: "title2"}]
        |> Ash.bulk_create(
          MnesiaPost,
          :create_with_after_action_error_and_after_transaction,
          transaction: :all,
          rollback_on_error?: true,
          return_errors?: true
        )

      # The after_action hook should have been called (it triggers the rollback)
      assert_receive {:after_action_error_hook_called}

      # The transaction was rolled back, returns a BulkResult with error status
      assert %Ash.BulkResult{status: :error, errors: [error]} = result
      assert %Ash.Error.Unknown.UnknownError{error: "\"after_action hook error\""} = error

      # after_transaction hook is NOT called because we don't have access to
      # the changesets after a transaction rollback
      refute_receive {:after_transaction_called, _}

      # No records should exist because the transaction was rolled back
      assert [] == MnesiaPost |> Ash.read!()
    end

    test "after_transaction hooks run outside batch transaction - no warning" do
      # With transaction: :batch, after_transaction hooks now run OUTSIDE the transaction
      # so no warning should be logged
      log =
        capture_log(fn ->
          result =
            Ash.bulk_create(
              [%{title: "test1"}, %{title: "test2"}],
              MnesiaPost,
              :create_with_after_transaction,
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
