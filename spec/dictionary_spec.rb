require_relative "../lib/dictionary"

RSpec.describe Dictionary do
  describe ".is_two_word?" do
    it "returns true when string can be split by dictionary words" do
      expect(Dictionary.is_composite_word?("двесотни", ["две", "сотни", "тысячи"])).to be true
    end

    it "returns true when string can be split by dictionary words other way" do
      expect(Dictionary.is_composite_word?("сотнидве", ["две", "сотни", "тысячи"])).to be true
    end

    it "returns false when no valid segmentation is possible" do
      expect(Dictionary.is_composite_word?("двесотни", ["сто", "тысячи"])).to be false
    end

    it "returns true on query with 3 valid words" do
      expect(Dictionary.is_composite_word?("дветысячисотни", ["две", "тысячи", "сотни"])).to be true
    end

    it "returns false when query starts with word but rest cannot form second word" do
      dictionary = ["две", "сотни"]
      query = "двехвост"
      expect(Dictionary.is_composite_word?(query, dictionary)).to be false
    end

    it "returns false when empty dictionary" do
      expect(Dictionary.is_composite_word?("двесотни", [])).to be false
    end

    it "returns false when empty string" do
      expect(Dictionary.is_composite_word?("", ["две", "сотни"])).to be false
    end

    it "handles overlapping words correctly" do
      expect(Dictionary.is_composite_word?("котик", ["кот", "ик", "коти", "тик"])).to be true
    end
  end
end
