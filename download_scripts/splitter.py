
base_url = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/"

output_f = open("pubmed_urls.txt", 'w+')

with open('raw.txt', 'r') as f:
    for line in f:
        fn = line.split('"')[1]

        assert 'tar' in fn

        if 'xml' in fn:
            output_f.write(f'{base_url}{fn}\n')

output_f.close()
