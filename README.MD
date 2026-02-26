# 🏥 FHIR Data Pipeline & Analytics Dashboard

A complete end-to-end health informatics project that downloads, parses, stores, and visualizes synthetic FHIR (Fast Healthcare Interoperability Resources) data. Built with Python, SQLite, and Streamlit.

![Python](https://img.shields.io/badge/python-3.12-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-active-success.svg)

## 📋 Project Overview

This project demonstrates production-level skills in healthcare data engineering and analytics:

- **ETL Pipeline**: Automated download, parsing, transformation, and loading of FHIR data
- **Database Design**: Normalized SQLite schema with foreign keys and indexes
- **SQL Analytics**: 26 complex queries covering population health, utilization, medications, labs, and risk
- **Interactive Dashboard**: 5-page Streamlit app with enhanced Plotly animations
- **Data Scale**: 555 patients, 215K observations, 28K encounters, 24K prescriptions, 17K conditions

## 🎯 Key Features

### 📊 Analytics Dashboard Pages

1. **Population Overview** - Age/gender pyramids, disease prevalence, demographic insights
2. **Clinical Utilization** - Encounter patterns, high utilizers, seasonal trends
3. **Medication Insights** - Top prescriptions, polypharmacy analysis (46.8% rate!)
4. **Lab Analytics** - Normal/abnormal tracking, patient trajectories over time
5. **Risk Predictions** - 30-day readmission rates (40%!), complexity scoring

### ✨ Enhanced Animations

- **Cumulative disease burden** - Watch conditions stack as population ages
- **Encounter bubble chart** - Seasonal patterns evolving year-by-year
- **Medication journey** - See polypharmacy develop over time
- **Lab trajectories** - Patients crossing normal→abnormal thresholds
- **Risk stratification** - Complexity shifting with chronic condition count

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- pip3

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/FHIR_Pipeline_Project.git
cd FHIR_Pipeline_Project

# Install dependencies
pip3 install -r requirements.txt

# Run the complete pipeline (optional - database already included)
cd src
python3 download_data.py    # Downloads FHIR data
python3 parse_fhir.py        # Parses JSON to DataFrames
python3 load_database.py     # Loads data to SQLite

# Launch the dashboard
cd ..
streamlit run dashboard/app.py
```

The dashboard will open at `http://localhost:8501`

## 📁 Project Structure

```
FHIR_Pipeline_Project/
├── src/
│   ├── download_data.py     # Downloads & extracts synthetic FHIR data
│   ├── parse_fhir.py         # Parses 557 JSON files → DataFrames
│   ├── load_database.py      # Creates SQLite database with schema
│   └── analytics.py          # 26 SQL query functions
├── dashboard/
│   └── app.py                # 5-page Streamlit dashboard
├── data/
│   ├── raw/fhir/             # 557 FHIR JSON files (gitignored)
│   └── fhir_data.db          # SQLite database (84 MB, gitignored)
├── requirements.txt          # Python dependencies
├── .gitignore                # Git exclusions
└── README.md                 # This file
```

## 🗄️ Database Schema

### Tables

- **patients** (555 rows) - Demographics, race, ethnicity, location
- **conditions** (17,253 rows) - Diagnoses with SNOMED codes
- **observations** (215,044 rows) - Vitals, labs, social history
- **encounters** (27,812 rows) - Ambulatory, emergency, inpatient visits
- **medication_requests** (24,256 rows) - Prescriptions with RxNorm codes

### Relationships

All clinical tables have `patient_id` foreign keys with indexes for fast JOINs.

## 📈 Key Findings

- **46.8% polypharmacy rate** - 260 patients on 5+ medications
- **40% overall 30-day readmission rate**
- **43.76% inpatient readmission rate** - major intervention opportunity
- **Top condition**: Stress (416 patients, 75% of population)
- **Most prescribed**: Acetaminophen, Ibuprofen, Amoxicillin

## 🔬 Analytics Functions

### Population Health (5 queries)
- Age-gender distribution
- Top conditions by prevalence
- Cumulative condition burden by age
- Race and geographic distribution

### Clinical Utilization (5 queries)
- Encounter type breakdown (AMB/EMER/IMP)
- Encounters per patient distribution
- High utilizers (50+ encounters)
- Temporal trends with seasonal patterns
- Conditions driving encounters

### Medications (4 queries)
- Top prescribed medications
- Polypharmacy distribution
- Medications by condition
- Patient medication timelines

### Lab Analytics (4 queries)
- Lab value distributions
- Abnormal lab detection with reference ranges
- Trends by age group
- Patient-specific lab timelines

### Risk & Readmission (2 queries)
- 30-day readmission analysis
- Patient complexity scoring (conditions + meds + encounters + abnormal labs)

## 🛠️ Technologies

- **Python 3.12** - Core language
- **Pandas** - Data manipulation
- **SQLite3** - Relational database
- **Streamlit** - Dashboard framework
- **Plotly** - Interactive visualizations with animations
- **Requests** - HTTP downloads

## 📚 Data Source

Synthetic FHIR R4 data from [Synthea](https://synthetichealth.github.io/synthea/) - realistic but completely artificial patient records for development and testing.

## 💡 Skills Demonstrated

### Data Engineering
✅ ETL pipeline design and implementation
✅ Data validation and error handling
✅ Database schema design with normalization
✅ Index optimization for query performance

### SQL & Analytics
✅ Complex JOINs and CTEs
✅ Window functions and aggregations
✅ Date/time calculations
✅ Self-joins for readmission detection

### Healthcare Informatics
✅ FHIR standard data parsing
✅ SNOMED/LOINC/RxNorm code systems
✅ Population health metrics
✅ Risk stratification algorithms
✅ Readmission prediction

### Data Visualization
✅ Interactive dashboards
✅ Animated storytelling with Plotly
✅ Clinical decision support displays
✅ Responsive layouts

## 🎓 Learning Resources

- [FHIR Documentation](https://www.hl7.org/fhir/)
- [Synthea Data Generator](https://github.com/synthetichealth/synthea)
- [Streamlit Docs](https://docs.streamlit.io/)
- [Plotly Python](https://plotly.com/python/)

## 📝 License

MIT License - feel free to use for learning, portfolios, or commercial projects.

## 🤝 Contributing

This is a portfolio/educational project, but suggestions and improvements are welcome! Open an issue or submit a PR.

## 📧 Contact

Built as a demonstration of healthcare data engineering and analytics skills.

---

**⚠️ Note**: This project uses synthetic data. No real patient information is included.

**🎯 Perfect for**: Data engineering portfolios, health informatics coursework, learning FHIR standards, SQL practice, dashboard design.
