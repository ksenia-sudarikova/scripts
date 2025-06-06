require "spec_helper"
require_relative "../lib/data_sorter"

describe DataSorter do
  let(:input) { "spec/fixtures/data_small.csv" }
  let(:output) { "tmp/output.txt" }
  let(:data_sorter) { DataSorter.new(input, output) }

  context "#parse_line" do
    let(:good_data) do
      {
        timestamp: "2023-09-03T12:45:00Z",
        transaction_id: "txn12345",
        user_id: "user987",
        amount: "500.25"
      }
    end

    it "raises error when wrong data" do
      line = good_data.map do |k, v|
        if k == :user_id
          "v,add_comma_to_user"
        else
          v
        end
      end.join(",")

      expect { data_sorter.parse_line(line) }.to raise_error(Exception)
    end

    it "parses line correctly" do
      line = "2023-09-03T12:45:00Z,txn12345,user987,500.25"
      txn = data_sorter.parse_line(line)
      expect(txn.transaction_id).to eq("txn12345")
      expect(txn.amount).to eq(500.25)
    end

    it "return transaction with good data" do
      line = good_data.map { |k, v| v }.join(",")

      txn = data_sorter.parse_line(line)
      expect(txn.timestamp).to eq(good_data[:timestamp])
      expect(txn.transaction_id).to eq(good_data[:transaction_id])
      expect(txn.user_id).to eq(good_data[:user_id])
      expect(txn.amount).to eq(good_data[:amount].to_f)
    end
  end

  context "#run" do
    before do
      data_sorter.run
    end

    context "small chunk_size" do
      let(:data_sorter) { DataSorter.new(input, output, {chunk_size: 2}) }

      it "return max on top" do
        lines = File.readlines(output)
        expect(lines.first).to include("503.00")
      end

      it "return min on bottom" do
        lines = File.readlines(output)
        expect(lines.last).to include("60.00")
      end

      it "does external sort correctly" do
        lines = File.readlines(output)
        sorted = lines.map { |line| data_sorter.parse_line(line) }.map { |h| h.amount }
        expect(sorted).to eq([503.0, 401.1, 401.0, 100.0, 99.0, 95.0, 90.0, 80.0, 60.0])
      end
    end

    context "default chunk_size" do
      it "return max on top" do
        lines = File.readlines(output)
        expect(lines.first).to include("503.00")
      end

      it "return min on bottom" do
        lines = File.readlines(output)
        expect(lines.last).to include("60.00")
      end

      it "does external sort correctly" do
        lines = File.readlines(output)
        sorted = lines.map { |line| data_sorter.parse_line(line) }.map { |h| h.amount }
        expect(sorted).to eq([503.0, 401.1, 401.0, 100.0, 99.0, 95.0, 90.0, 80.0, 60.0])
      end
    end
  end
end
