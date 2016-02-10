### sitestat tool
sitestat tool designed to catch statistics from various CMS sites.
The underlying process follow these steps:

- Fetch all site names from SiteDB
- loop over specific time range, e.g. last 3m
  - create dates for that range
- Use popularity API (DSStatInTImeWindow) 
  to get summary statistics. The API returns various information about dataset
  usage on sites.
- Organize data in number of access bins
- For every bin collect dataset names
- Call DBS APIs to get dataset statistics via blocksummaries API.
- sum up info about file_size which will give total size used by specific T1/T2 site.
