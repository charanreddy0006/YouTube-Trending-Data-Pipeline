Bronze Bucket Name = data-pipeline-bronze-youtube 
Silver Bucket Name = data-pipeline-silver-youtube
Gold Bucket Name =data-pipeline-gold-youtube

Script Bucket = data-pipeline-script-youtube

SNS ARN = arn:aws:sns:ap-south-1:069129260793:yt-data-pipeline-alerts:b00aa550-a198-4bc8-a1b7-a87d091f4199


Glue Bronze DB - yt-pipeline-bronze
Glue Silver DB - yt-pipeline-silver
Glue Gold DBB - yt-pipeline-gold

--- Glue Jobs

--bronze_database - yt-pipeline-bronze
--bronze_table - raw_statistics
--silver_bucket -  data-pipeline-silver-youtube
--silver_database - yt-pipeline-silver
--silver_table - clean_statistics

--gold_bucket - data-pipeline-gold-youtube
--gold_database - yt-pipeline-gold