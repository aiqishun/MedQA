## Data Directory

Due to licensing restrictions of the original MEDQA dataset, derived datasets
under `data/` are not included in this repository.

All cardiology-related subsets can be reproduced by running:

```bash
python scripts/extract_heart_disease.py
python scripts/convert_mcq_to_eval.py
Generated files will be placed under:
data/derived/
including:

heart_disease.jsonl – raw cardiology subset

heart_disease_all.jsonl – full set for knowledge modeling

heart_disease_mcq.jsonl – MCQ-only evaluation set

cardio_eval.jsonl – standardized evaluation format
操作：
cat >> README.md <<'TXT'

## Data Directory

Due to licensing restrictions of the original MEDQA dataset, derived datasets
under `data/` are not included in this repository.

All cardiology-related subsets can be reproduced by running:

```bash
python scripts/extract_heart_disease.py
python scripts/convert_mcq_to_eval.py
Generated files will be placed under:

data/derived/

including:

heart_disease.jsonl – raw cardiology subset

heart_disease_all.jsonl – full set for knowledge modeling

heart_disease_mcq.jsonl – MCQ-only evaluation set

cardio_eval.jsonl – standardized evaluation format

TXT

然后提交：

```bash
git add README.md
git commit -m "document data directory and reproduction pipeline"
git push
