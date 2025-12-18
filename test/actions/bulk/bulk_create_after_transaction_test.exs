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

      create :create_with_after_transaction_raises_exception do
        change AfterTransactionRaisesException
      end

      create :create_with_stop_on_error_hook do
        change AfterTransactionWithStopOnError
      end

      create :upsert_with_after_transaction do
        upsert? true
        upsert_identity :unique_title
        upsert_fields [:title2]

        change after_transaction(fn
                 _changeset, {:ok, result}, _context ->
                   send(self(), {:upsert_after_transaction_called, result.id, result.title})
                   {:ok, result}

                 _changeset, {:error, error}, _context ->
                   send(self(), {:upsert_after_transaction_error, error})
                   {:error, error}
               end)
      end

      create :upsert_with_condition_and_after_transaction do
        upsert? true
        upsert_identity :unique_title
        upsert_fields [:title2]
        upsert_condition expr(false)

        change after_transaction(fn
                 _changeset, {:ok, result}, _context ->
                   send(
                     self(),
                     {:upsert_skipped_after_transaction_called, result.id, result.title}
                   )

                   {:ok, result}

                 _changeset, {:error, error}, _context ->
                   {:error, error}
               end)
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

    identities do
      identity :unique_title, :title do
        pre_check_with Ash.Test.Domain
      end
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

    test "hooks execute even if return_records? is not set" do
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
      assert_receive {:after_transaction_create_called, _}

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

  describe "after_transaction exception handling" do
    test "exception in hook is caught and converted to error" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create with hook that raises an exception
      result =
        Ash.bulk_create(
          [%{title: "title1"}],
          Post,
          :create_with_after_transaction_raises_exception,
          tenant: org.id,
          return_errors?: true,
          return_records?: true
        )

      # The hook should have been called (before raising)
      assert_receive {:before_exception_raise, _id}, 1000

      # Exception should be caught and converted to an error
      assert result.status == :error
      assert result.error_count == 1
      assert length(result.errors) == 1

      # Verify the error contains information about the exception
      [error] = result.errors
      assert Exception.message(error) =~ "Hook intentionally raised exception"
    end

    test "hook raising exception doesn't crash bulk operation with multiple records" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create multiple records with stop_on_error?: false to process all records
      # Exception in hook should be handled gracefully for each record
      result =
        Ash.bulk_create(
          [%{title: "title1"}, %{title: "title2"}, %{title: "title3"}],
          Post,
          :create_with_after_transaction_raises_exception,
          tenant: org.id,
          stop_on_error?: false,
          return_errors?: true,
          return_records?: true
        )

      # All hooks should have been attempted
      assert_receive {:before_exception_raise, _id1}, 1000
      assert_receive {:before_exception_raise, _id2}, 1000
      assert_receive {:before_exception_raise, _id3}, 1000

      # All should fail but operation shouldn't crash
      assert result.status == :error
      assert result.error_count == 3
    end
  end

  describe "after_transaction with stop_on_error? (default: true)" do
    test "hook returns error without return_stream? - error captured in result" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create record where hook will return an error (title contains "stop_here")
      # stop_on_error?: true is the default in test config
      result =
        Ash.bulk_create(
          [%{title: "stop_here_please"}],
          Post,
          :create_with_stop_on_error_hook,
          tenant: org.id,
          return_errors?: true,
          return_records?: true
        )

      # Hook should have been called and returned error
      assert_receive {:hook_error_for_stop_on_error, _id}, 1000

      # Error should be captured
      assert result.status == :error
      assert result.error_count == 1
    end

    test "hook returns error stops processing with default stop_on_error?: true" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create multiple records with stop_on_error?: true (default)
      # When hook returns error for second record, processing should stop
      result =
        Ash.bulk_create(
          [%{title: "good_record_1"}, %{title: "stop_here_fail"}, %{title: "good_record_2"}],
          Post,
          :create_with_stop_on_error_hook,
          tenant: org.id,
          return_errors?: true,
          return_records?: true
        )

      # First record succeeds, second fails with hook error, third not processed
      assert_receive {:hook_success_for_stop_on_error, _id1}, 1000
      assert_receive {:hook_error_for_stop_on_error, _id2}, 1000
      # Third record NOT processed due to stop_on_error?: true
      refute_receive {:hook_success_for_stop_on_error, _id3}, 100

      # With stop_on_error?: true, status is :error when any error occurs
      # (even though first record succeeded)
      assert result.status == :error
      assert result.error_count == 1
    end

    test "stop_on_error?: false continues after hook error" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create multiple records with stop_on_error?: false
      # Hook will fail for "stop_here" records but should continue
      result =
        Ash.bulk_create(
          [%{title: "good_1"}, %{title: "stop_here_fail"}, %{title: "good_2"}],
          Post,
          :create_with_stop_on_error_hook,
          tenant: org.id,
          stop_on_error?: false,
          return_errors?: true,
          return_records?: true
        )

      # All hooks should be called
      assert_receive {:hook_success_for_stop_on_error, _id1}, 1000
      assert_receive {:hook_error_for_stop_on_error, _id2}, 1000
      assert_receive {:hook_success_for_stop_on_error, _id3}, 1000

      # Partial success - continues despite error
      assert result.status == :partial_success
      assert result.error_count == 1
      assert length(result.records) == 2
    end
  end

  defmodule ManualCreateSimple do
    @moduledoc """
    Simple manual create module that just performs the create.
    After_transaction hooks are added via a separate change module.
    """
    use Ash.Resource.ManualCreate

    def create(changeset, _opts, _context) do
      # Perform the actual create using ETS data layer
      Ash.DataLayer.Ets.create(changeset.resource, changeset)
    end

    def bulk_create(changesets, _opts, _context) do
      Enum.map(changesets, fn changeset ->
        case Ash.DataLayer.Ets.create(changeset.resource, changeset) do
          {:ok, record} ->
            record =
              Ash.Resource.put_metadata(
                record,
                :bulk_create_index,
                changeset.context.bulk_create.index
              )

            {:ok, record}

          {:error, error} ->
            {:error, error}
        end
      end)
    end
  end

  defmodule ManualCreateFails do
    @moduledoc """
    Manual create module that always fails.
    Used to test after_transaction hook error handling with manual actions.
    """
    use Ash.Resource.ManualCreate

    def create(_changeset, _opts, _context) do
      {:error, "intentional manual create error"}
    end
  end

  defmodule ManualAfterTransactionChange do
    @moduledoc """
    Change module that adds after_transaction hook for manual action testing.
    """
    use Ash.Resource.Change

    def change(changeset, _opts, _context) do
      Ash.Changeset.after_transaction(changeset, fn _cs, result ->
        case result do
          {:ok, record} ->
            send(self(), {:manual_after_transaction_success, record.id})
            {:ok, record}

          {:error, error} ->
            send(self(), {:manual_after_transaction_error, error})
            {:error, error}
        end
      end)
    end
  end

  defmodule ManualAfterTransactionConvertsErrorChange do
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
            send(self(), {:manual_after_transaction_converted_error})
            # Return a fake success
            {:ok, %{changeset.data | id: Ash.UUID.generate(), title: "manual_recovered"}}
        end
      end)
    end
  end

  defmodule ManualPost do
    @moduledoc false
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      notifiers: [Notifier]

    alias Ash.Test.Actions.BulkCreateAfterTransactionTest.{
      Org,
      ManualCreateSimple,
      ManualCreateFails,
      ManualAfterTransactionChange,
      ManualAfterTransactionConvertsErrorChange
    }

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
      defaults [:read, :destroy]

      create :create do
        accept [:title, :title2, :org_id]
      end

      create :create_manual_with_after_transaction do
        accept [:title, :title2, :org_id]
        manual ManualCreateSimple
        change ManualAfterTransactionChange
      end

      create :create_manual_with_after_transaction_converts_error do
        accept [:title, :title2, :org_id]
        manual ManualCreateFails
        change ManualAfterTransactionConvertsErrorChange
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
    end
  end

  describe "after_transaction with manual actions" do
    test "after_transaction hooks work with manual action (single record)" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [%{title: "manual_test_1"}],
          ManualPost,
          :create_manual_with_after_transaction,
          tenant: org.id,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 1

      # Verify after_transaction hook was called
      assert_receive {:manual_after_transaction_success, _id}, 1000
    end

    test "after_transaction hooks work with manual action (multiple records)" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [%{title: "manual_bulk_1"}, %{title: "manual_bulk_2"}, %{title: "manual_bulk_3"}],
          ManualPost,
          :create_manual_with_after_transaction,
          tenant: org.id,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 3

      # Verify after_transaction hooks were called for all records
      assert_receive {:manual_after_transaction_success, _id1}, 1000
      assert_receive {:manual_after_transaction_success, _id2}, 1000
      assert_receive {:manual_after_transaction_success, _id3}, 1000
    end

    test "after_transaction hook can convert error to success in manual action" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create(
          [%{title: "will_fail"}],
          ManualPost,
          :create_manual_with_after_transaction_converts_error,
          tenant: org.id,
          return_records?: true,
          return_errors?: true
        )

      # The hook should have been called
      assert_receive {:manual_after_transaction_converted_error}, 1000

      # The result should show success (error converted by hook)
      assert result.status == :success
      assert length(result.records) == 1
      assert hd(result.records).title == "manual_recovered"
    end

    test "manual action after_transaction hooks work with return_stream?" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result_stream =
        Ash.bulk_create(
          [%{title: "stream_manual_1"}, %{title: "stream_manual_2"}],
          ManualPost,
          :create_manual_with_after_transaction,
          tenant: org.id,
          return_stream?: true,
          return_records?: true
        )

      results = Enum.to_list(result_stream)
      assert length(results) == 2

      # Verify hooks executed
      assert_receive {:manual_after_transaction_success, _id1}, 1000
      assert_receive {:manual_after_transaction_success, _id2}, 1000
    end

    test "manual action after_transaction hooks work with transaction: :all" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [%{title: "tx_all_manual_1"}, %{title: "tx_all_manual_2"}],
          ManualPost,
          :create_manual_with_after_transaction,
          tenant: org.id,
          transaction: :all,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 2

      # Verify hooks executed
      assert_receive {:manual_after_transaction_success, _id1}, 1000
      assert_receive {:manual_after_transaction_success, _id2}, 1000
    end
  end

  describe "after_transaction with load option" do
    test "load option with return_stream? and after_transaction hooks" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create an author to reference
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Test Author"})
        |> Ash.create!()

      result_stream =
        Ash.bulk_create(
          [
            %{title: "load_stream_1", author_id: author.id},
            %{title: "load_stream_2", author_id: author.id}
          ],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          return_stream?: true,
          return_records?: true,
          load: [:author],
          authorize?: false
        )

      results = Enum.to_list(result_stream)
      assert length(results) == 2

      # Verify hooks executed
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000

      # Verify author relationship is loaded
      for {:ok, record} <- results do
        assert %Author{name: "Test Author"} = record.author
      end
    end

    test "load option with transaction: :all and after_transaction hooks" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create an author to reference
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Test Author"})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [
            %{title: "load_tx_all_1", author_id: author.id},
            %{title: "load_tx_all_2", author_id: author.id}
          ],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          transaction: :all,
          return_records?: true,
          load: [:author],
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 2

      # Verify hooks executed
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000

      # Verify author relationship is loaded
      for record <- result.records do
        assert %Author{name: "Test Author"} = record.author
      end
    end

    test "load option with transaction: :batch (default) and after_transaction hooks" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create an author to reference
      author =
        Author
        |> Ash.Changeset.for_create(:create, %{name: "Test Author"})
        |> Ash.create!()

      result =
        Ash.bulk_create!(
          [
            %{title: "load_tx_batch_1", author_id: author.id},
            %{title: "load_tx_batch_2", author_id: author.id}
          ],
          Post,
          :create_with_after_transaction_hook,
          tenant: org.id,
          # transaction: :batch is the default
          return_records?: true,
          load: [:author],
          authorize?: false
        )

      assert result.status == :success
      assert length(result.records) == 2

      # Verify hooks executed
      assert_receive {:after_transaction_create_called, _id1}, 1000
      assert_receive {:after_transaction_create_called, _id2}, 1000

      # Verify author relationship is loaded
      for record <- result.records do
        assert %Author{name: "Test Author"} = record.author
      end
    end
  end

  describe "after_transaction with upsert" do
    test "hooks execute on successful upsert insert" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # Create new records via upsert (insert path)
      result =
        Ash.bulk_create!(
          [%{title: "upsert_test_1", title2: "v1"}, %{title: "upsert_test_2", title2: "v1"}],
          Post,
          :upsert_with_after_transaction,
          tenant: org.id,
          return_records?: true
        )

      assert result.status == :success
      assert length(result.records) == 2

      # after_transaction hooks should have been called for both inserts
      assert_receive {:upsert_after_transaction_called, _id1, "upsert_test_1"}, 1000
      assert_receive {:upsert_after_transaction_called, _id2, "upsert_test_2"}, 1000
    end

    test "hooks execute on upsert with conflict (update path)" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # First create a record
      result1 =
        Ash.bulk_create!(
          [%{title: "conflict_test", title2: "original"}],
          Post,
          :upsert_with_after_transaction,
          tenant: org.id,
          return_records?: true
        )

      assert result1.status == :success
      [original_post] = result1.records
      assert_receive {:upsert_after_transaction_called, original_id, "conflict_test"}, 1000

      # Now upsert with same title - should trigger update path
      result2 =
        Ash.bulk_create!(
          [%{title: "conflict_test", title2: "updated"}],
          Post,
          :upsert_with_after_transaction,
          tenant: org.id,
          return_records?: true
        )

      assert result2.status == :success
      [updated_post] = result2.records

      # Should be the same record (upsert updated it)
      assert updated_post.id == original_post.id
      assert updated_post.title2 == "updated"

      # after_transaction hook should have been called for the update
      assert_receive {:upsert_after_transaction_called, ^original_id, "conflict_test"}, 1000
    end

    test "upsert_skipped with after_transaction hook and return_skipped_upsert?: false" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # First create a record
      result1 =
        Ash.bulk_create!(
          [%{title: "skip_test", title2: "original"}],
          Post,
          :upsert_with_condition_and_after_transaction,
          tenant: org.id,
          return_records?: true
        )

      assert result1.status == :success
      [original_post] = result1.records
      assert_receive {:upsert_skipped_after_transaction_called, _id, "skip_test"}, 1000

      # Now upsert with same title but upsert_condition: false
      # This should skip the upsert (not update)
      result2 =
        Ash.bulk_create!(
          [%{title: "skip_test", title2: "should_not_update"}],
          Post,
          :upsert_with_condition_and_after_transaction,
          tenant: org.id,
          return_records?: true,
          return_skipped_upsert?: false
        )

      # When upsert is skipped and return_skipped_upsert? is false, status is still success
      assert result2.status == :success
      # No records returned (skipped)
      assert result2.records == []

      # Verify the original wasn't updated
      refreshed = Ash.get!(Post, original_post.id, tenant: org.id)
      assert refreshed.title2 == "original"

      # The hook should NOT be called for skipped upserts
      refute_receive {:upsert_skipped_after_transaction_called, _, _}
    end

    test "upsert with return_skipped_upsert?: true and hooks" do
      org =
        Org
        |> Ash.Changeset.for_create(:create, %{})
        |> Ash.create!()

      # First create a record
      result1 =
        Ash.bulk_create!(
          [%{title: "return_skip_test", title2: "original"}],
          Post,
          :upsert_with_condition_and_after_transaction,
          tenant: org.id,
          return_records?: true
        )

      assert result1.status == :success
      [original_post] = result1.records
      assert_receive {:upsert_skipped_after_transaction_called, _id, "return_skip_test"}, 1000

      # Now upsert with return_skipped_upsert?: true
      result2 =
        Ash.bulk_create!(
          [%{title: "return_skip_test", title2: "should_not_update"}],
          Post,
          :upsert_with_condition_and_after_transaction,
          tenant: org.id,
          return_records?: true,
          return_skipped_upsert?: true
        )

      # Status should still be success
      assert result2.status == :success

      # With return_skipped_upsert?: true, we get the existing record back
      assert length(result2.records) == 1
      [returned_post] = result2.records
      assert returned_post.id == original_post.id
      # The title2 should be the original (not updated)
      assert returned_post.title2 == "original"

      # For skipped upserts with return_skipped_upsert?: true,
      # the after_transaction hook IS called with the existing record
      assert_receive {:upsert_skipped_after_transaction_called, _, "return_skip_test"}, 1000
    end
  end
end
