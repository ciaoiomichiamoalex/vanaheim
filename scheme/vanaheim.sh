# run recording
python -c "import sys; sys.path.extend(['c:/source/vanaheim', 'c:/source/vanaheim/share', 'c:/source/vanaheim/src'])" && python c:/source/vanaheim/src/recording_doc.py

# make schema backup
pg_dump -U postgres -h 127.0.0.1 -p 5432 -d postgres -n vanaheim -F c -f "c:/source/vanaheim/scheme/reserves/vanaheim_$(date -f 'yyyy_MM_dd').dump"
