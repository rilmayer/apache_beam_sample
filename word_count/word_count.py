import re
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def run():

    pipeline_options = PipelineOptions([
        '--runner=DirectRunner'
    ])

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (p
                 | beam.Create([
                    'cat dog cow, wolf',
                    'cat dog bear',
                    'dog ape elephant']))
        
        counts = (lines
         | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
         | beam.Map(lambda x: (x, 1))
         | beam.CombinePerKey(sum)
         )

        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)

        output = counts | 'Format' >> beam.Map(format_result)

        def print_output(output):
            print(output, type(output))

        output | beam.Map(print_output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()