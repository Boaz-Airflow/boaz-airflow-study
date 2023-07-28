# boaz-airflow-study

2023 빅데이터 대학생(원) 동아리 BOAZ의 여름방학 Airflow 스터디입니다.

- 교재 : Apache Airflow 기반의 데이터 파이프라인 (바스 하렌슬락, 율리안 더라위터르)

- 스터디 기간: 2023.07.15 ~ 2023.09.xx
- dags 폴더 안에 각 주차에 해당하는 폴더를 만들고 실습 코드를 올려주세요
    - ex) week1 폴더 안에 practice.py (dags/week1/practice.py)

- 각자의 이름으로 된 브랜치를 만들어서 코드를 올려주세요
    - cd dags
    - git switch -c feat/add-(여러분의이름을영어로)
    - git add 실습파일
    - git commit -m "feat: add practice.py"
    - git push  

- push 이후 PR을 날려주세요
    - https://github.com/Boaz-Airflow/boaz-airflow-study/pulls 으로 이동! <br>
    PR의 Title이나 Description은 자유롭게 적어주신 후 PR을 생성해주시면, 제가 스터디 중이나 스터디할 때, 확인 후 Merge 합니다 :D

- 만약 dag에서 parsing이 되지 않기를 바란다면 dags/.airflowignore 에 파일과 폴더를 추가해주세요
