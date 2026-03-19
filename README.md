# craft-data-engineering

Welcome to the **craft-data-engineering** repository. This is a foundational project for building robust, clean, and efficient data engineering pipelines.

## 10-Page Software Craftsmanship Guide

The centerpiece of this repo is a comprehensive guide to Software Craftsmanship in Data Engineering. 

- **PDF Guide**: [Software_Craftsmanship_Guide.pdf](Software_Craftsmanship_Guide.pdf)
- **Content Sources**: Found in [content/guide/](content/guide/) (YAML format).

### Key Concepts Covered
1. **Mindset**: Ousterhout's Philosophy of Software Design, Clean Code, and Professionalism.
2. **Productivity**: Modern Python tooling (`uv`, `Ruff`), `Raycast`, and Agentic AI IDEs.
3. **Architecture**: Hexagonal Architecture (Ports & Adapters) and Domain-Driven Design.
4. **Data Contracts**: Quality at the source with Chad Sanderson's concepts and `dbt` model contracts.
5. **Testing**: Advanced validation with **Great Expectations (GX)** and Testcontainers.
6. **Clean Code**: SOLID principles tailored for Data and "Refactoring constant".
7. **Idempotence**: Reliable incremental strategies (Merge, Overwrite) and **CDC**.
8. **IaC & Security**: Terraform, Secrets Management, and Least Privilege.
9. **Observability**: **Elementary** (dbt-native) and the **Elastic Stack**.
10. **DataOps**: CI/CD automation, Blue/Green deployments, and Docker multi-stage builds.

## Concrete Examples

### Reducing Cognitive Load (Ousterhout Approach)
We provide a comparative example of "Obscure" vs "Obvious" code:
- **Code Nightmare vs Data Craftsman**: [content/examples/transaction_processor.py](content/examples/transaction_processor.py)
  - This example illustrates how `Dataclasses` and `Enum` can eliminate "Unknown Unknowns" and reduce the cognitive burden on developers.

## Project Automation

Use the [justfile](justfile) to manage the environment:
- `just setup`: Initialize the local environment.
- `just generate`: Re-generate the PDF guide from YAML sources.

---
Created with ❤️ by Axel Mauroy (Data Craftsman)
