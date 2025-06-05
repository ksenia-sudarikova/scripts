require "set"

class Dictionary
  def self.is_composite_word?(query = "двесотни", dictionary = ["две", "сотни", "тысячи"])
    return false if query.empty?

    stack = [[query, Set.new]]

    until stack.empty?
      current_query, used_words = stack.pop

      dictionary.each do |word|
        next if used_words.include?(word)
        next unless current_query.start_with?(word)

        rest = current_query[word.length..]
        new_used_words = used_words.dup.add(word)

        return true if rest.empty? && new_used_words.size >= 2

        stack.push([rest, new_used_words]) unless rest.empty?
      end
    end

    false
  end
end
