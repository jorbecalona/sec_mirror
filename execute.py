import sec_parser
import preprocessor
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os
import re
import pandas as pd
from functools import partial


def get_html(file_path):
    with open(file_path, 'r') as fh:
        contents = fh.read()
        html = re.search(r'<html>.+?</html>', contents, re.DOTALL | re.I)
    if html:
        return html.group()
    else:
        return False

def write_to_file(contents, file_path):
    if not contents:
        return
    dir_path = '/'+'/'.join(file_path.split('/')[0:-1])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(file_path, 'w') as fh:
        fh.write(contents)
    return

def get_file_paths(dir_path, extension=None):
    file_paths = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if extension:
                if file.endswith(extension):
                    file_paths.append(os.path.join(root, file))
            else:
                file_paths.append(os.path.join(root, file))
    return file_paths


def execute_html_parse(html_file_path, parsed_dir_path, preprocess=True,
                       fuzzy_threshold=0.8, marked_html=False, max_num_missing_items=0):
    if os.path.isfile(html_file_path):
        with open(html_file_path, 'r') as fh:
            contents = fh.read()
        file_name = html_file_path.split('/')[-1]
        parse_dict, marked_html_str = sec_parser.parse_items(contents, fuzzy_threshold=fuzzy_threshold, marked_html=marked_html,
                                        max_num_missing_items=max_num_missing_items)
        if parse_dict:
            print('parsing completed successfully for file: ', file_name)
            print()
            if preprocess:
                parse_dict = {label: preprocessor.preprocess_html(html_str) for label, html_str in parse_dict.items()}
                print('pre-processing completed successfully for file: ', file_name)
            for label, item_html in parse_dict.items():
                new_file_name = file_name.replace('.htm', '_' + label + '.htm')
                new_file_path = parsed_dir_path+file_name.replace('.htm','')+'/'+new_file_name
                write_to_file(item_html, new_file_path)
            new_file_name = file_name.replace('.htm', '_' + 'marked' + '.htm')
            new_file_path = parsed_dir_path + file_name.replace('.htm', '') + '/' + new_file_name
            write_to_file(marked_html_str, new_file_path)
            return True
        else:
            print('parsing failed for file: ', file_name)
            print()
            return False
    else:
        return False


def execute_parallel(dfrow, preprocess=True, fuzzy_threshold=0.8, marked_html=False, max_num_missing_items=0):
    file_path = dfrow['file_path']
    print('processing file: ', file_path)
    print()
    try:
        if file_path.endswith('.txt'):
            html_file_path = dfrow['html_dir_path'] + file_path.split('/')[-1].replace('.txt', '.htm')
            write_to_file(get_html(file_path), html_file_path)
            file_path = html_file_path
        execute_html_parse(file_path, dfrow['parsed_dir_path'],
                preprocess=preprocess, fuzzy_threshold=fuzzy_threshold, marked_html=marked_html, max_num_missing_items=max_num_missing_items)
        return True
    except:
        print('something went wrong during processing of file: ', file_path)
        print()
        return False


def apply_parallel(df, func, get=dask.multiprocessing.get, npartitions=7):
    ddf = dd.from_pandas(df, npartitions=npartitions, sort=False)
    with ProgressBar():
        return ddf.apply(func, meta=df.columns, axis=1).compute(get=get)


def main():
    root_project_path = '/Users/dimitryslavin/Dropbox/all_docs/Education/UM_PhD_Docs/phd_research/sec_firm_mapping_clean/'
    file_dir_path = root_project_path+'data/10k_sample/raw_text_10k/'
    html_dir_path = root_project_path+'data/10k_sample/data_html_test/'
    parsed_dir_path = root_project_path+'data/10k_sample/data_parsed_test/'
    results_file_path = root_project_path+'data/10k_sample/parse_results.csv'
    file_paths = get_file_paths(html_dir_path, extension = '.htm')

    combos = list(zip(file_paths, [html_dir_path]*len(file_paths), [parsed_dir_path]*len(file_paths)))
    combos = pd.DataFrame.from_records(combos, columns=['file_path', 'html_dir_path', 'parsed_dir_path'])
    combos['parse_result'] = apply_parallel(combos, partial(execute_parallel, max_num_missing_items = 0, marked_html = True), npartitions=6)

    combos.to_csv(results_file_path, index=False)

main()