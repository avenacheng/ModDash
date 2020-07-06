# Data Processing

Preprocessing Steps:

1. Rename columns for easy understanding and create columns for year, month, and day.
2. Remove rows where the author deleted their account and/or removed their comment.
3. Find sentiment scores of comment:
    * Calculate VADER (sentiment) score using NLTK's vader lexicon
4. Create comments table and write to PostgreSQL.
