import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm

def is_korean(char):
    """한글인지 확인하는 함수"""
    return ord('가') <= ord(char) <= ord('힣') or ord('ㄱ') <= ord(char) <= ord('ㅎ')

def extract_datetime_from_filename(filename):
    """파일명에서 날짜시간 추출"""
    try:
        # 20240305075359_20240305075239167.txt 형식에서 첫 번째 날짜시간 추출
        datetime_str = filename.split('_')[0]
        if len(datetime_str) == 14:  # YYYYMMDDHHMMSS
            return f"{datetime_str[:4]}-{datetime_str[4:6]}-{datetime_str[6:8]} {datetime_str[8:10]}:{datetime_str[10:12]}:{datetime_str[12:]}"
    except:
        return None
    return None

def extract_column_and_value(line):
    """라인에서 컬럼명과 값을 추출하는 함수"""
    parts = line.strip().split()
    if len(parts) < 2:
        return None, None
        
    # 한글이 나오는 위치 찾기
    korean_idx = -1
    for i, part in enumerate(parts):
        if any(is_korean(c) for c in part):
            korean_idx = i
            break
    
    # 컬럼명 추출 (한글 전까지의 모든 영문)
    if korean_idx != -1:
        column_parts = parts[:korean_idx]
    else:
        column_parts = parts[:-1]
    
    column_name = ' '.join(column_parts).strip()
    
    # 값 추출
    value = parts[-1] if parts else None
        
    return column_name, value

def process_dialysis_files(base_directory):
    all_data = []
    all_columns = set()
    
    folders = [f for f in os.listdir(base_directory) if os.path.isdir(os.path.join(base_directory, f))]
    
    for folder_name in tqdm(folders, desc="Processing folders", unit="folder"):
        folder_path = os.path.join(base_directory, folder_name)
        txt_files = [f for f in os.listdir(folder_path) if f.endswith('.txt')]
        
        for filename in tqdm(txt_files, desc=f"Processing {folder_name}", unit="file", leave=False):
            file_path = os.path.join(folder_path, filename)
            
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = {
                        'device_id': folder_name,
                        'filename': filename
                    }
                    
                    # 첫 줄에서 시간 정보 추출
                    first_line = file.readline()
                    try:
                        timestamp_str = first_line.strip().split()[-1]
                        if len(timestamp_str.split()) == 1:  # HH:MM:SS 형식인 경우
                            # 파일명에서 날짜 추출
                            datetime_str = extract_datetime_from_filename(filename)
                            if datetime_str:
                                data['EQTIME'] = datetime_str
                            else:
                                continue
                        else:
                            data['EQTIME'] = timestamp_str
                    except:
                        continue
                    
                    # 나머지 라인 처리
                    for line in file:
                        if not line.strip():
                            continue
                            
                        column_name, value = extract_column_and_value(line)
                        
                        if column_name and value:
                            # Operating Phase와 같은 특별한 경우 처리
                            if column_name == "Operating Phase":
                                # 한글 다음의 모든 값을 합침
                                korean_idx = -1
                                parts = line.strip().split()
                                for i, part in enumerate(parts):
                                    if any(is_korean(c) for c in part):
                                        korean_idx = i
                                        break
                                if korean_idx != -1 and korean_idx < len(parts) - 1:
                                    value = ' '.join(parts[korean_idx + 1:])
                            
                            # 컬럼 목록에 추가
                            all_columns.add(column_name)
                            
                            try:
                                # 숫자형 데이터 변환 시도
                                if value.replace('.', '').replace('-', '').isdigit():
                                    data[column_name] = float(value)
                                else:
                                    data[column_name] = value
                            except ValueError:
                                data[column_name] = value

                    all_data.append(data)
                    
            except Exception as e:
                print(f"\nError processing file {file_path}: {str(e)}")
                continue
    
    print("\nCreating DataFrame and sorting data...")
    
    df = pd.DataFrame(all_data)
    
    base_columns = ['device_id', 'filename', 'EQTIME', 'Operating Phase', 'Remaining Time']
    other_columns = sorted(list(all_columns - set(base_columns)))
    final_columns = [col for col in base_columns + other_columns if col in df.columns]
    df = df[final_columns]
    
    print("Converting EQTIME to datetime...")
    df['EQTIME'] = pd.to_datetime(df['EQTIME'])
    
    df = df.sort_values(['device_id', 'EQTIME'])
    
    return df

# 메인 실행 코드
if __name__ == "__main__":
    base_directory = '/Users/guno/Downloads/202403'
    
    print("Starting data processing...")
    df = process_dialysis_files(base_directory)
    
    output_path = os.path.join(os.path.dirname(base_directory), 'exalis_data_all_devices.csv')
    print(f"\nSaving data to {output_path}...")
    df.to_csv(output_path, index=False)
    print("Data saved successfully!")
    
    print("\nData Summary:")
    print(f"Total records: {len(df):,}")
    print(f"Number of devices: {df['device_id'].nunique()}")
    print(f"Number of columns: {len(df.columns)}")
    print(f"Time range: {df['EQTIME'].min()} ~ {df['EQTIME'].max()}")
    
    print("\nRecords per device:")
    device_counts = df['device_id'].value_counts()
    for device, count in device_counts.items():
        print(f"{device}: {count:,} records")
        
    print("\nColumns in the dataset:")
    for col in df.columns:
        print(f"- {col}")