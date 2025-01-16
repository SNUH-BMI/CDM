import numpy as np
import pandas as pd
import glob
import tarfile
import matplotlib.pyplot as plt
import os
from datetime import datetime

def find_lox_directories(root_path):
    """
    Finds all directories containing .LOX files
    """
    lox_dirs = []
    for dirpath, dirnames, filenames in os.walk(root_path):
        if any(f.endswith('.LOX') for f in filenames):
            lox_dirs.append(dirpath)
    return lox_dirs

def group_files_by_folder(file_list):
    """
    파일들을 폴더명과 연도별로 그룹화
    """
    grouped_files = {}
    for file_path in file_list:
        path_parts = file_path.split('\\')
        folder_name = path_parts[-3]  # ex: PA000000
        year = path_parts[-2]         # ex: 2024
        key = (folder_name, year)
        
        if key not in grouped_files:
            grouped_files[key] = []
        grouped_files[key].append(file_path)
    return grouped_files

extmap = {
    "pci": ("Network config", ["ascii", "ini", "split", "strip"]),
    "pca": None,  # Some binary data, skip it
    "pcu": ("Therapy config", ["ascii", "ini", "split", "strip"]),
    "pcm": ("Machine config", ["ascii", "split"]),
    "plr": ("System events", ["ascii", "split", "strip", "noemptylines"]),
    "ple": ("User events", ["utf-16", "split", "strip", "csv"]),
    "plp": ("Pressure", ["utf-8", "split", "strip", "csv"]),
    "pls": ("Fluids", ["ascii", "split", "strip", "csv", ]),
    "ply": ("Syringe", ["ascii", "split", "strip", "csv", ]),
    "plc": ("PLC", ["ascii", "split", "strip", "csv", ]),
    "plt": ("Tare", ["ascii", "split", "strip", "csv", ]),
    "pli": ("PLI", ["ascii", "split", "strip", "csv", ]),
    "pll": ("PLL", ["ascii", "split", "strip", "csv", ])
}

def get_loxfile_data(fname):
    """
    Returns all the data contained in the loxfile
    :param fname: path to the lox file
    :return: dictionary containing the extracted data
    """
    ret = {}

    if not os.path.exists(fname):
        print(f"File not found: {fname}")
        return ret
    
    if os.path.getsize(fname) == 0:
        print(f"Empty file: {fname}")
        return ret

    try:
        tar = tarfile.open(fname, "r:gz")
        try:
            members = tar.getnames()
        except Exception as e:
            print(f"Error reading members from {fname}: {str(e)}")
            tar.close()
            return ret

        for member in members:
            try:
                try:
                    _ign, ext = map(str.lower, member.split("."))
                except ValueError:
                    print(f"Invalid filename format in {fname}: {member}")
                    continue

                if ext not in extmap:
                    continue

                if extmap[ext] is None:
                    continue

                try:
                    f = tar.extractfile(member)
                    if f is None:
                        print(f"Could not extract {member} from {fname}")
                        continue
                    
                    desc, extra = extmap[ext]
                    data = f.read()

                    for elem in extra:
                        try:
                            if elem == "strip":
                                data = [x.strip() for x in data]
                            elif elem in ["utf-8", "utf-16", "ascii"]:
                                data = data.decode(elem)
                            elif elem == "split":
                                data = data.split("\n")
                            elif elem == "csv":
                                data = [x.split(';') for x in data]
                            elif elem == "noemptylines":
                                data = [x for x in data if x]
                        except Exception as e:
                            print(f"Error processing {elem} for {member} in {fname}: {str(e)}")
                            continue

                    ret[desc] = data

                except Exception as e:
                    print(f"Error processing {member} in {fname}: {str(e)}")
                    continue

            except Exception as e:
                print(f"Unexpected error processing member in {fname}: {str(e)}")
                continue

        tar.close()
        return ret

    except tarfile.ReadError as e:
        print(f"Error reading tar file {fname}: {str(e)}")
        return ret
    except Exception as e:
        print(f"Unexpected error with file {fname}: {str(e)}")
        return ret

# 기본 경로 설정
root_path = "E:\\CRRT\\Baxter"
base_save_path = os.path.join(root_path, "merge")

# 만약 merge 폴더가 없다면 생성
if not os.path.exists(base_save_path):
    os.makedirs(base_save_path)

# 최종 병합을 위한 DataFrame 초기화
final_events = None
final_metadata = None
current_sess = 1  # 전체 세션 번호 추적용

# .LOX 파일이 있는 모든 디렉토리 찾기
base_paths = find_lox_directories(root_path)

print("Found directories containing .LOX files:")
for path in base_paths:
    path_count = len(glob.glob(os.path.join(path, '*.LOX')))
    print(f"{path}: {path_count} files")

# 전체 파일 리스트 생성
list_file = []
for path in base_paths:
    path_files = glob.glob(os.path.join(path, '*.LOX'))
    list_file.extend(path_files)

print(f"\nTotal number of files found: {len(list_file)}")

# 파일들을 폴더별로 그룹화
grouped_files = group_files_by_folder(list_file)

list_type = ['416', '550', '17', '22', '20', '21', '24', '16', '20', '279', '5', '19', '21']
list_type_t = ['환자 인식 번호:', '요법 종류:', '혈액', '사전 혈액 펌프', '대체용액', '투석액',
               '환자 수분 제거', '치료가 시작되었습니다(실행 모드).', '재시작을 선택했습니다.',
               '보고: 필터 응고가 진행중', '경고: 필터 응고됨', '중지를 선택했습니다.', '치료 종료를 선택했습니다.']
list_type_cod = ['PT_ID','CRRT_type','BFR','Pre','Replace','Dialysate','UF',
                 'HD_start','HD_restart', 'Warning_coag', 'Filter_coag','HD_suspend','HD_end']

# 각 그룹별로 처리
for (folder_name, year), files in grouped_files.items():
    print(f"\nProcessing {folder_name} {year}...")
    list_file_group = files
    
    # Event 데이터 처리
    merged_event = None
    for n, t_file in enumerate(list_file_group):
        print(f"Processing file {n+1}/{len(list_file_group)}: {t_file}")
        
        machine_name = t_file.split('\\')[-3]
        dict_file = get_loxfile_data(t_file)

        if not dict_file or 'User events' not in dict_file:
            print(f"Skipping file {t_file} - No valid user events data")
            continue

        try:
            try:
                table_event = pd.DataFrame(dict_file['User events'][27:], 
                                         columns=dict_file['User events'][26])
            except:
                table_event = pd.DataFrame(dict_file['User events'][27:], 
                                         columns=dict_file['User events'][26]+['None'])

            table_event = table_event.iloc[:,1:]
            table_event = table_event[table_event['Time'].astype(str).str.strip() != '']
            table_event['Time'] = table_event['Time'].astype(str).str[:-2] + '00'
            
            try:
                table_event['Time'] = pd.to_datetime(table_event['Time'])
            except Exception as e:
                print(f"Error converting time in {t_file}: {str(e)}")
                continue
                
            table_event.sort_values(by='Time', inplace=True)
            table_event.reset_index(drop=True, inplace=True)

            all_col = None
            for i in range(len(list_type)):
                try:
                    curr_type = list_type[i]
                    curr_type_t = list_type_t[i]
                    curr_type_name = list_type_cod[i]
                    
                    col_sub = table_event[
                        (table_event['Type(cod)'] == curr_type) & 
                        (table_event['Type'] == curr_type_t)
                    ][['Time', 'Sample']]
                    
                    col_sub['Sample'] = np.where(col_sub['Sample'].astype(str) == '', 'O', col_sub['Sample'])
                    col_sub.rename(columns={'Sample': curr_type_name}, inplace=True)
                    
                    if all_col is None:
                        all_col = col_sub
                    else:
                        all_col = pd.merge(all_col, col_sub, on='Time', how='outer')
                except Exception as e:
                    print(f"Error processing type {curr_type_name} in {t_file}: {str(e)}")
                    continue

            if all_col is not None:
                all_col.sort_values(by='Time', inplace=True)
                all_col['Machine'] = machine_name

                if merged_event is None:
                    merged_event = all_col
                else:
                    merged_event = pd.concat([merged_event, all_col], axis=0)

        except Exception as e:
            print(f"Error processing file {t_file}: {str(e)}")
            continue

    if merged_event is not None:
        merged_event.sort_values(by=['Machine', 'Time'], inplace=True)
        merged_event.drop_duplicates(inplace=True)
        merged_event.reset_index(drop=True, inplace=True)

        # Session 처리
        merged_event['Sess'] = 0
        list_machine = merged_event['Machine'].drop_duplicates()
        sess_num = 1

        for n, machine in enumerate(list_machine):
            machine_event = merged_event[merged_event['Machine'] == machine].copy()
            start_array = (~machine_event['PT_ID'].isna()) & (machine_event['HD_start'] == 'O')
            end_array = ~machine_event['HD_end'].isna()
            idx_array = np.zeros(machine_event.shape[0])
            find_start = True
            find_pos = 0
            find_end = machine_event.shape[0]
            curr_start = None
            curr_end = None
            complete = 0

            while find_pos != find_end:
                if find_start:
                    checker = start_array.iloc[find_pos]
                    if checker:
                        if complete == 1:
                            idx_array[curr_start:curr_end+1] = sess_num
                            complete = 0
                            sess_num += 1
                        curr_start = find_pos
                        find_start = False
                    else:
                        checker = end_array.iloc[find_pos]
                        if checker:
                            curr_end = find_pos
                        find_pos += 1
                else:
                    checker = end_array.iloc[find_pos]
                    if checker:
                        curr_end = find_pos
                        complete = 1
                        find_start = True
                    find_pos += 1

            if complete == 1:
                idx_array[curr_start:curr_end+1] = sess_num
                complete = 0
                sess_num += 1

            merged_event.loc[merged_event['Machine'] == machine,'Sess'] = idx_array

        # Metadata 처리
        print("\nProcessing metadata...")
        merged_metadata = None
        for n, t_file in enumerate(list_file_group):
            print(f"Processing metadata file {n+1}/{len(list_file_group)}")
            try:
                machine_name = t_file.split('\\')[-3]
                dict_file = get_loxfile_data(t_file)

                if not dict_file:
                    continue

                if 'Fluids' in dict_file and 'Pressure' in dict_file:
                    table_fluid = pd.DataFrame(dict_file['Fluids'][7:], 
                                             columns=dict_file['Fluids'][6])
                    table_fluid = table_fluid.iloc[:,1:]
                    table_fluid['Time'] = pd.to_datetime(table_fluid['Time'])
                    table_fluid.sort_values(by='Time', inplace=True)

                    table_pressure = pd.DataFrame(dict_file['Pressure'][7:], 
                                               columns=dict_file['Pressure'][6])
                    table_pressure = table_pressure.iloc[:,1:]
                    table_pressure['Time'] = pd.to_datetime(table_pressure['Time'])
                    table_pressure.sort_values(by='Time', inplace=True)

                    table_metadata = pd.merge(table_fluid, table_pressure, 
                                            on='Time', how='outer')
                    table_metadata['Machine'] = machine_name

                    if merged_metadata is None:
                        merged_metadata = table_metadata
                    else:
                        merged_metadata = pd.concat([merged_metadata, table_metadata])

            except Exception as e:
                print(f"Error processing metadata for file {t_file}: {str(e)}")
                continue

        if merged_metadata is not None:
            merged_metadata.sort_values(by=['Machine', 'Time'], inplace=True)
            merged_metadata.drop_duplicates(inplace=True)
            merged_metadata.reset_index(drop=True, inplace=True)

            merged_metadata['Sess'] = 0
            for sess in merged_event['Sess'].unique():
                if sess == 0:
                    continue
                curr_hd = merged_event[merged_event['Sess'] == sess]
                t_start = curr_hd['Time'].iloc[0]
                t_end = curr_hd['Time'].iloc[-1]
                name_machine = curr_hd['Machine'].iloc[0]
                mask = ((merged_metadata['Time'] >= t_start) & 
                       (merged_metadata['Time'] <= t_end) & 
                       (merged_metadata['Machine'] == name_machine))
                merged_metadata.loc[mask, 'Sess'] = sess

            # Save results for this group
            if not os.path.exists(base_save_path):
                os.makedirs(base_save_path)

            valid_events = merged_event[merged_event['Sess'] != 0].copy()
            valid_metadata = merged_metadata[merged_metadata['Sess'] != 0].copy()

            # Sess 번호 재할당
            max_sess = valid_events['Sess'].max()
            sess_mapping = {old_sess: new_sess for old_sess, new_sess in 
                        zip(sorted(valid_events['Sess'].unique()), 
                            range(current_sess, current_sess + int(max_sess)))}
            valid_events['Sess'] = valid_events['Sess'].map(sess_mapping)
            valid_metadata['Sess'] = valid_metadata['Sess'].map(sess_mapping)

            # 다음 그룹의 시작 세션 번호 업데이트
            current_sess += int(max_sess)

            # 개별 파일 저장
            events_filename = f'merged_table_valid_{folder_name}_{year}.csv'
            metadata_filename = f'merged_metadata_{folder_name}_{year}.csv'

            valid_events.to_csv(os.path.join(base_save_path, events_filename), 
                            index=False)
            valid_metadata.to_csv(os.path.join(base_save_path, metadata_filename), 
                            index=False)
            
            if final_events is None:
                final_events = valid_events
            else:
                final_events = pd.concat([final_events, valid_events])
    
            if final_metadata is None:
                final_metadata = valid_metadata
            else:
                final_metadata = pd.concat([final_metadata, valid_metadata])
        else:
            print(f"No valid metadata for {folder_name} {year}")
    else:
        print(f"No valid events for {folder_name} {year}")

    print("\nAll processing complete!")

# 최종 병합 파일 저장
if final_events is not None and final_metadata is not None:
    final_events.sort_values(['Machine', 'Time']).reset_index(drop=True).to_csv(
        os.path.join(root_path, 'merged_table_valid_all.csv'), index=False)
    final_metadata.sort_values(['Machine', 'Time']).reset_index(drop=True).to_csv(
        os.path.join(root_path, 'merged_metadata_all.csv'), index=False)
 
    print("\nFinal Statistics:")
    print(f"Total sessions: {final_events['Sess'].max()}")
    print(f"Total unique patients: {final_events['PT_ID'].nunique()}")
    print(f"Total events: {len(final_events)}")
    print(f"Total metadata records: {len(final_metadata)}")
    print("\nFiles saved:")
    print(f"Individual files: {base_save_path}")
    print(f"Merged files: {root_path}")
    print("  - merged_table_valid_all.csv")
    print("  - merged_metadata_all.csv")
else:
    print("No valid data was processed")