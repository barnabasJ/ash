defmodule Ash.Test.Actions.ManyToManyTest do
  @moduledoc false
  use ExUnit.Case, async: true

  require Ash.Query

  defmodule PostLink do
    use Ash.Resource,
      data_layer: Ash.DataLayer.Ets

    actions do
      defaults [:create, :read, :update, :destroy]
    end

    ets do
      private? true
    end

    attributes do
      uuid_primary_key :id
      attribute :source_id, :uuid
      attribute :destination_id, :uuid
    end
  end

  defmodule OtherRegistry do
    use Ash.Registry

    entries do
      entry PostLink
    end
  end

  defmodule OtherApi do
    use Ash.Api

    resources do
      registry OtherRegistry
    end
  end

  defmodule SlugCalc do
    use Ash.Calculation

    @impl true
    def select(_, _, _) do
      [:title]
    end

    @impl true
    def calculate(%{title: title}, _, _) do
      IO.inspect("slug")

      title
      |> String.downcase()
      |> String.replace(~r/\s+/, "-")
    end
  end

  defmodule UrlCalc do
    use Ash.Calculation

    @impl true
    def load(_, _, _) do
      [:slug]
    end

    @impl true
    def calculate(%{slug: slug}, _, _) do
      "https://example.com/posts/#{slug}"
    end
  end

  defmodule Post do
    @moduledoc false
    use Ash.Resource,
      data_layer: Ash.DataLayer.Ets

    ets do
      private?(true)
    end

    actions do
      defaults [:read, :update, :destroy]

      create :create do
        primary? true

        argument :linked_posts, {:array, :map}

        change manage_relationship(:linked_posts, type: :create)
      end
    end

    attributes do
      uuid_primary_key :id
      attribute :title, :string
    end

    calculations do
      calculate :slug, :string, SlugCalc

      calculate :url,
                :string,
                UrlCalc
    end

    relationships do
      has_many :post_links, PostLink do
        destination_attribute :source_id
        api OtherApi
      end

      many_to_many :linked_posts, __MODULE__ do
        through PostLink
        join_relationship :post_links
        source_attribute_on_join_resource :source_id
        destination_attribute_on_join_resource :destination_id
      end
    end
  end

  defmodule Registry do
    @moduledoc false
    use Ash.Registry

    entries do
      entry(Post)
    end
  end

  defmodule Api do
    @moduledoc false
    use Ash.Api

    resources do
      registry Registry
    end
  end

  describe "in separate registries" do
    test "it allows managing without raising an error" do
      Post
      |> Ash.Changeset.for_create(:create, %{
        title: "buz",
        linked_posts: [%{title: "foo"}, %{title: "bar"}]
      })
      |> Api.create!()
    end

    @tag :wip
    test "it allows loading calculations on the related resources" do
      import Ash.Query, only: [expr: 1]

      post =
        Post
        |> Ash.Changeset.for_create(:create, %{
          title: "buz",
          linked_posts: [%{title: "foo"}, %{title: "bar"}]
        })
        |> Api.create!()

      post_id = post.id

      post =
        Post
        |> Ash.Query.for_read(:read)
        |> Ash.Query.filter(expr(id == ^post_id))
        |> Ash.Query.load(linked_posts: [:url])
        |> Api.read_one!()
        |> IO.inspect()

      assert %Ash.Test.Actions.ManyToManyTest.Post{
               title: "buz",
               id: ^post_id,
               linked_posts: [
                 %Ash.Test.Actions.ManyToManyTest.Post{
                   title: "bar",
                   url: "https://example.com/posts/bar"
                 },
                 %Ash.Test.Actions.ManyToManyTest.Post{
                   title: "foo",
                   url: "https://example.com/posts/foo"
                 }
               ]
             } = post
    end
  end
end
