require "spec_helper"
require_relative "../lib/data_sorter"

describe DataSorter do
  it "parses line correctly" do
    line = "2023-09-03T12:45:00Z,txn12345,user987,500.25"
    txn = DataSorter.parse_line(line)
    expect(txn.transaction_id).to eq("txn12345")
    expect(txn.amount).to eq(500.25)
  end

  it "raises error when wrong data" do
    line = "2023-09-03T12:45:00Z,txn12345,user_with_,_comma987,500.25"

    expect { DataSorter.parse_line(line) }.to raise_error(Exception)
  end

  it "sorts array of transactions by amount descending" do
    txns = [
      Transaction.new(nil, nil, nil, 200.0),
      Transaction.new(nil, nil, nil, 500.0),
      Transaction.new(nil, nil, nil, 100.0)
    ]
    sorted = DataSorter.merge_sort(txns)
    expect(sorted.map { |t| t.amount }).to eq([500.0, 200.0, 100.0])
  end

  it "does external sort correctly on small file" do
    input = "spec/fixtures/data_small.csv"
    output = "tmp/output.txt"

    DataSorter.sort_large_file(input, output, 3)
    lines = File.readlines(output)
    expect(lines.first).to include("503.00")
    expect(lines.last).to include("60.00")
  end

  it "does external sort correctly on small file" do
    input = "spec/fixtures/data_small.csv"
    output = "tmp/output.txt"

    DataSorter.sort_large_file(input, output, 2)
    lines = File.readlines(output)
    sorted = lines.map { |line| DataSorter.parse_line(line) }.map { |h| h.amount }
    puts sorted.inspect
    expect(sorted).to eq([503.0, 401.1, 401.0, 100.0, 99.0, 95.0, 90.0, 80.0, 60.0])
  end
end
