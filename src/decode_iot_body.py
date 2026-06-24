import json
import base64
import sys
from pathlib import Path

def decode_iot_file(filepath: str):
    content = Path(filepath).read_text(encoding='utf-8')
    lines = [l.strip() for l in content.strip().split('\n') if l.strip()]
    
    messages = []
    for i, line in enumerate(lines):
        raw = json.loads(line)
        body_decoded = base64.b64decode(raw['Body']).decode('utf-8')
        parsed_body = json.loads(body_decoded)
        
        # Aplatir les values en dict propre
        values = {
            v['id'].split('.')[-1]: v['value']
            for v in parsed_body.get('values', [])
        }
        
        messages.append({
            'message_index': i + 1,
            'enqueued_at': raw['EnqueuedTimeUtc'],
            'connection_device_id': raw['SystemProperties']['connectionDeviceId'],
            'machine_id': parsed_body.get('process_serial_number'),
            'box': parsed_body.get('box'),
            'values': values
        })
    
    return messages


def pretty_print(messages):
    for msg in messages:
        print(f"\n{'='*60}")
        print(f"Message #{msg['message_index']}")
        print(f"{'='*60}")
        print(f"  machine_id  : {msg['machine_id']}")
        print(f"  box         : {msg['box']}")
        print(f"  enqueued_at : {msg['enqueued_at']}")
        print(f"  device_id   : {msg['connection_device_id']}")
        print(f"  nb tags     : {len(msg['values'])}")
        print(f"\n  --- Valeurs ---")
        for k, v in msg['values'].items():
            print(f"  {k:<45} = {v}")


if __name__ == '__main__':
    input_dir = Path(r'C:\Users\u279306\dev\Architecture_Azure_Snowflake_Candling\data\test_json\brut')
    output_dir = Path(r'C:\Users\u279306\dev\Architecture_Azure_Snowflake\data\test_json\converted')
    output_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(input_dir.glob('*.json'))
    if not json_files:
        print(f"Aucun fichier JSON trouvé dans {input_dir}")
        sys.exit(0)

    for filepath in json_files:
        print(f"\nTraitement : {filepath.name}")
        messages = decode_iot_file(str(filepath))
        pretty_print(messages)

        output_path = output_dir / (filepath.stem + '_decoded.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)
        print(f"\n✓ JSON décodé exporté : {output_path}")

