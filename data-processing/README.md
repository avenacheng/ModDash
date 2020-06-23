# Data Processing

Preprocessing Steps:

1. Rename columns for easy understanding and create columns for year, month, and day.
2. Remove rows where the author deleted their account and/or removed their comment.
3. Find sentiment scores of comment:
    * Remove punctuation to create a "clean comment"
    * Calculate VADER (sentiment) score
4. Create comments and posts table with relevant fields.
5. For posts table, calculate the number of negative comments, total comments, average sentiment, and percentage of toxic comments per post/
