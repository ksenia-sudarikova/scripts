require "spec_helper"
require_relative "../lib/merge_sort"

describe MergeSort do
  it "sorts by amount descending" do
    data = [[100, "a"], [300, "b"], [200, "c"]]
    expect(MergeSort.run(data)).to eq([[300, "b"], [200, "c"], [100, "a"]])
  end

  it "handles empty array" do
    expect(MergeSort.run([])).to eq([])
  end

  it "returns single element as is" do
    expect(MergeSort.run([[42, "only"]])).to eq([[42, "only"]])
  end

  it "sorts array with duplicate amounts" do
    data = [[100, "a"], [100, "b"], [200, "c"]]
    result = MergeSort.run(data)
    expect(result[0][0]).to eq(200)
    expect(result[1][0]).to eq(100)
    expect(result[2][0]).to eq(100)
    expect(result.map(&:last)).to match_array(%w[a b c])
  end

  it "sorts with negative and zero amounts" do
    data = [[0, "zero"], [-100, "neg"], [50, "pos"]]
    expect(MergeSort.run(data)).to eq([[50, "pos"], [0, "zero"], [-100, "neg"]])
  end

  it "keep order of equal amounts" do
    data = [[100, "a"], [100, "b"], [200, "c"]]
    result = MergeSort.run(data)
    expect(result[1..2].map(&:last)).to eq(%w[a b]) # if stable
  end

  it "handles large input correctly" do
    data = (1..1001).map { |i| [i, "v#{i}"] }.shuffle
    sorted = MergeSort.run(data)
    expect(sorted.map(&:first)).to eq((1..1001).to_a.reverse)
  end
end
