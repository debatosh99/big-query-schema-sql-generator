# wordcount.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run_wordcount_pipeline(input_file, output_prefix):
    with beam.Pipeline(options=PipelineOptions()) as p:
        lines = p | 'Read' >> beam.io.ReadFromText(input_file)
        counts = (
            lines
            | 'Split' >> beam.FlatMap(lambda line: line.split())
            | 'PairWithOne' >> beam.Map(lambda word: (word, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
        )
        counts | 'Write' >> beam.io.WriteToText(output_prefix)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input file path')
    parser.add_argument('--output', dest='output', required=True, help='Output file prefix')
    args = parser.parse_args()
    run_wordcount_pipeline(args.input, args.output)
