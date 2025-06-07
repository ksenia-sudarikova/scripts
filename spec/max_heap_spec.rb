require "spec_helper"
require_relative "../lib/max_heap"
require_relative "../lib/data_sorter"

describe MaxHeap do
  it "returns nil when heap empty" do
    heap = MaxHeap.new
    expect(heap.pop).to be_nil
  end

  it "move negative priorities lower" do
    heap = MaxHeap.new
    heap.add(-5, "low")
    heap.add(10, "high")
    expect(heap.pop).to eq("high")
  end

  it "handles zero as a valid priority" do
    heap = MaxHeap.new
    heap.add(0, "zero")
    heap.add(1, "one")
    expect(heap.pop).to eq("one")
    expect(heap.pop).to eq("zero")
  end

  it "handles nil values" do
    heap = MaxHeap.new
    heap.add(1, nil)
    expect(heap.pop).to be_nil
  end

  it "keep order for equal priorities" do
    heap = MaxHeap.new
    heap.add(5, "a")
    heap.add(5, "b")
    expect([heap.pop, heap.pop]).to match_array(["a", "b"])
  end

  context "max heap with transactions" do
    let(:heap) { MaxHeap.new }

    before do
      transactions = [
        Transaction.new(nil, nil, nil, 200.0),
        Transaction.new(nil, nil, nil, 500.0),
        Transaction.new(nil, nil, nil, 100.0)
      ]
      transactions.each do |transaction|
        heap.add(transaction.amount, transaction)
      end
    end

    it "return transactions by amount descending" do
      expect(heap.pop.amount).to eq(500.0)
      expect(heap.pop.amount).to eq(200.0)
      expect(heap.pop.amount).to eq(100.0)
    end
  end

  it "correctly sorts in max heap" do
    heap = MaxHeap.new
    [5, 3, 8, 1].each { |n| heap.add(n, "val#{n}") }
    expect(heap.pop).to eq("val8")
    expect(heap.pop).to eq("val5")
    expect(heap.pop).to eq("val3")
    expect(heap.pop).to eq("val1")
    expect(heap.pop).to eq(nil)
  end

  it "handles large number of items" do
    heap = MaxHeap.new
    10_000.times { |i| heap.add(i, "val#{i}") }
    expect(heap.pop).to eq("val9999")
  end
end
