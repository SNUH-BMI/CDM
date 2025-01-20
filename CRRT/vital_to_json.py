import vitaldb
import numpy as np
import pandas as pd
from datetime import datetime
import os
import json
 
# .vital 파일 저장되어 있는 경로
base_path = r'C:\vitaldb'

# JSON 저장할 경로
json_save_path = r'C:\vitaldb\json_files'
 
if not os.path.exists(json_save_path):
    os.makedirs(json_save_path)
 
def parse_vital_filename(filename):
    """vital 파일명에서 정보 추출"""
    parts = filename.split('_')
    if len(parts) >= 4:
        icu = parts[0]         
        patient_id = parts[1]  
        date = parts[2]        
        time = parts[3].split('.')[0]  
        return {
            'icu': icu,
            'patient_id': patient_id,
            'date': date,
            'time': time,
            'datetime': datetime.strptime(f"{date}_{time}", "%y%m%d_%H%M%S")
        }
    return None
 
def get_numeric_tracks(file_path):
    """파일에 존재하는 데이터 트랙 찾기"""
    try:
        vf = vitaldb.read_vital(file_path)
        all_tracks = vitaldb.vital_trks(file_path)
        numeric_tracks = []

        for track in all_tracks:
            try:
                data = vf.to_numpy([track], 1)
                if data is not None and len(data) > 0 and np.issubdtype(data.dtype, np.number):
                    if np.any(~np.isnan(data)):
                        numeric_tracks.append(track)
            except:
                continue

        return numeric_tracks
    except Exception as e:
        print(f"트랙 확인 중 에러 발생: {e}")
        return []
 
vital_files = [f for f in os.listdir(base_path) if f.endswith('.vital')]
 
file_info = []
for file in vital_files:
    info = parse_vital_filename(file)
    if info:
        info['filename'] = file
        file_info.append(info)
 
# 환자별 그룹화
patient_groups = {}
for info in file_info:
    key = f"{info['icu']}_{info['patient_id']}_{info['date']}"
    if key not in patient_groups:
        patient_groups[key] = []
    patient_groups[key].append(info)
 
# 각 그룹 내에서 시간순 정렬
for key in patient_groups:
    patient_groups[key].sort(key=lambda x: x['datetime'])
 
# 각 환자 데이터 처리 및 JSON 저장
for patient_key, files in patient_groups.items():
    print(f"\n=== patient: {patient_key} ===")
    print(f"연속된 파일 수: {len(files)}")

    # dictionary
    patient_data = {
        'patient_id': patient_key,
        'start_time': files[0]['datetime'].strftime("%Y-%m-%d %H:%M:%S"),
        'end_time': files[-1]['datetime'].strftime("%Y-%m-%d %H:%M:%S"),
        'file_count': len(files),
        'tracks': {}
    }

    all_numeric_tracks = set()
    for file_info in files:
        file_path = os.path.join(base_path, file_info['filename'])
        numeric_tracks = get_numeric_tracks(file_path)
        all_numeric_tracks.update(numeric_tracks)

    print(f"발견된 트랙 수: {len(all_numeric_tracks)}")

    # 각 트랙별로 시계열 데이터 수집
    for track in all_numeric_tracks:
        track_data = []

        for file_info in files:
            try:
                file_path = os.path.join(base_path, file_info['filename'])
                vf = vitaldb.read_vital(file_path)
                data = vf.to_numpy([track], 1)

                if data is not None and len(data) > 0:
                    time_index = pd.date_range(
                        start=file_info['datetime'],
                        periods=len(data),
                        freq='S'
                    )

                    # NaN이 아닌 값만 저장
                    valid_mask = ~np.isnan(data.flatten())
                    valid_times = time_index[valid_mask]
                    valid_values = data.flatten()[valid_mask]

                    # 데이터 포인트 추가
                    track_data.extend([
                        {
                            'timestamp': t.strftime("%Y-%m-%d %H:%M:%S"),
                            'value': float(v)
                        }
                        for t, v in zip(valid_times, valid_values)
                    ])

            except Exception as e:
                print(f"{track} 처리 중 에러 발생: {e}")

        if track_data:
            patient_data['tracks'][track] = {
                'data': track_data,
                'count': len(track_data),
                'mean': float(np.mean([d['value'] for d in track_data])),
                'std': float(np.std([d['value'] for d in track_data]))
            }

    # JSON 파일로 저장
    json_filename = os.path.join(json_save_path, f"{patient_key}.json")
    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(patient_data, f, ensure_ascii=False, indent=2)
        print(f"JSON 저장 완료: {json_filename}")

        print(f"저장된 트랙 수: {len(patient_data['tracks'])}")
        for track_name, track_info in patient_data['tracks'].items():
            print(f"- {track_name}: {track_info['count']} 데이터 포인트")

    except Exception as e:
        print(f"JSON 저장 중 에러 발생: {e}")
 
