
# AWS-DE-Projects @ Amalitech

This repository hosts AWS-focused data engineering projects undertaken during my professional engagements at Amalitech. Each project will be maintained in separate branches within this repository, providing a structured approach to version control, learning documentation, and potential collaboration.

---

## Objectives

* To build robust data engineering solutions using **AWS-native tools and services**.
* To deepen understanding of **data engineering fundamentals**, including data ingestion, transformation, orchestration, and monitoring.
* To document practical applications, key learnings, and resolutions to challenges encountered across AWS-focused projects.

---

## Branching Strategy

The repository adheres to a clean branching structure:

```
main
├── README.md (project overview)
├── prod_project1
│   └── final implementation files for project 1 (merged from dev_project1)
├── prod_project2
│   └── final implementation files for project 2 (merged from dev_project2)
├── prod_projectN
│   └── final implementation files for project N (merged from dev_projectN)
```

* **main**: Contains this README and serves as the entry point for understanding the repository. No active development files reside here.
* **prod\_projectX**: Stable, production-ready branch for each AWS-related project.
* **dev\_projectX**: Temporary development branch used while implementing and testing each project.

  * Merged into the corresponding `prod_projectX` branch upon completion.
  * Deleted post-merge to keep the repo history clean and focused.

---

## Workflow

1. Start a new AWS-related project in a `dev_projectX` branch.
2. Conduct all development (e.g., scripts, Terraform/CDK templates, Glue jobs, MWAA DAGs) within this branch.
3. Upon successful completion and validation, merge into `prod_projectX`.
4. Delete the development branch to preserve a tidy commit history.

This structure ensures:

* Clear delineation between development and production-ready code.
* Simpler debugging, rollback, and collaboration.
* A standardized approach across all AWS-based data engineering projects.

---

## Tools & Technologies

* **Languages**: Python, SQL, Bash

* **AWS Services**:

  * **Data Integration & ETL**: AWS Glue, AWS Lambda
  * **Orchestration**: Amazon MWAA (Managed Apache Airflow)
  * **Storage**: S3, RDS, Redshift
  * **Streaming**: Kinesis, Kafka on MSK
  * **Security & IAM**: IAM Roles, KMS
  * **Infrastructure as Code**: AWS CDK, Terraform

* **Other Tools**: Apache Airflow, DBT, Pandas, Docker, Git

---

## Contribution Guidelines

Although this repository is maintained primarily for individual work and knowledge growth, all projects follow industry best practices such as:

* Clear and consistent commit messages.
* Structured and descriptive branch naming.
* Detailed documentation and code comments.

This promotes discipline and readiness for production-grade collaboration.

---

## Future Focus Areas

Anticipated projects will target the following themes:

* Event-driven data workflows with AWS Lambda & EventBridge
* Scalable ETL with AWS Glue & Step Functions
* Pipeline deployment using MWAA and CI/CD integration
* Real-time streaming using Kinesis or MSK
* Data governance and cataloging via AWS Glue Data Catalog & Lake Formation

Stay tuned as new projects are added and iterated on in their respective `prod_projectX` branches.

---

Let me know if you'd like a version of this with placeholders you can easily reuse (`project_template`, `dev_template`, etc.) or if you're using specific AWS accounts or environments I should reflect.
