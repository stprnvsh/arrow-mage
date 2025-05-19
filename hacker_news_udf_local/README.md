
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_hacker_news_udf import hacker_news_udf

# Instantiate individual jobs
job_hacker_news_udf = hacker_news_udf(story_type='top', limit=5)

# Instantiate multi-step job
job = fused.experimental.job([job_hacker_news_udf])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
