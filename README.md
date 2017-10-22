# WikiPageRank-Spark-EC2

This project is to implement the PageRank to find the most important Wikipedia pages on the provided
Wikipedia dataset using Spark/GraphX on Amazon EC2. Programming languages: Scala, Java, Python.
1. Data Set
1.1 Data Description
We choose the Freebase Wikipedia Extraction (WEX), a processed dump of the English version
of Wikipedia. The complete WEX dump is approximately 64GB uncompressed, and contains
both XML and text versions of each page. We only need use the “articles” files from the Data
Set (31G), which contains one line per wiki article in a tab-separated format. Each line in WEX
data set (freebase-wex-2009-01-12-articles.tsv) has five fields, separated by tabs:
• page id
• page title
• last date the page was modified
• XML version of the page
• plain text version of the page

Task 1) Basic: Get the top 100 Pages.
Task 2) Advanced: Performance analysis, comparison using pure Spark and GraphX
Task 3) Get the top 100 universities and their properties (PageRank/weights, and
others: e.g., top alumni etc.).
