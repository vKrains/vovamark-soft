import os
import glob

# Папка, где лежат файлы

target_dir = r"D:/Софт/скрипты и аутпутс/Списки поставок"

# Шаблон для поиска .xlsx файлов
pattern = os.path.join(target_dir, "*.xlsx")

files = glob.glob(pattern)

if not files:
    print("Нет файлов .xlsx для удаления.")
else:
    deleted_count = 0
    for file_path in files:
        try:
            os.remove(file_path)
            print(f"Удалён: {file_path}")
            deleted_count += 1
        except Exception as e:
            print(f"Ошибка при удалении {file_path}: {e}")
    print(f"Удалено файлов: {deleted_count}")
