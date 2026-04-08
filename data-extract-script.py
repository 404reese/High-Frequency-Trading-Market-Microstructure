import gzip

input_file = "data/01302020.NASDAQ_ITCH50.gz"
output_file = "data/sample_itch_100mb.bin"

with gzip.open(input_file, 'rb') as f_in:
    # Read first 100MB
    chunk = f_in.read(100 * 1024 * 1024) 
    with open(output_file, 'wb') as f_out:
        f_out.write(chunk)

print(f"Sample created: {output_file}")