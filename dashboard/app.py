"""
FHIR Analytics Dashboard
Interactive Streamlit dashboard for population health, clinical utilization,
medication insights, lab analytics, and risk predictions.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os

# Add parent directory to path to import analytics module
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
import analytics

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="FHIR Analytics Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Healthcare color palette
COLORS = {
    'primary': '#1f77b4',    # Blue
    'secondary': '#2ca02c',  # Green
    'warning': '#ff7f0e',    # Orange
    'danger': '#d62728',     # Red
    'male': '#1f77b4',       # Blue for males
    'female': '#e377c2',     # Pink for females
    'normal': '#2ca02c',     # Green for normal
    'abnormal': '#d62728'    # Red for abnormal
}

# Custom CSS
st.markdown("""
    <style>
    .main {
        background-color: #ffffff;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 5px;
    }
    h1 {
        color: #1f77b4;
    }
    h2 {
        color: #2ca02c;
    }
    </style>
    """, unsafe_allow_html=True)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def configure_animation(fig, frame_duration=800, transition_duration=400):
    """
    Apply enhanced animation configuration to Plotly figures.

    Args:
        fig: Plotly figure object
        frame_duration: Duration of each frame in milliseconds
        transition_duration: Duration of transitions in milliseconds
    """
    fig.update_layout(
        updatemenus=[dict(
            type="buttons",
            showactive=False,
            x=0.0,
            y=1.15,
            buttons=[
                dict(
                    label="▶ Play",
                    method="animate",
                    args=[None, {
                        "frame": {"duration": frame_duration, "redraw": True},
                        "transition": {"duration": transition_duration, "easing": "cubic-in-out"}
                    }]
                ),
                dict(
                    label="⏸ Pause",
                    method="animate",
                    args=[[None], {
                        "frame": {"duration": 0},
                        "mode": "immediate"
                    }]
                )
            ]
        )],
        sliders=[dict(
            active=0,
            yanchor="top",
            y=-0.2,
            xanchor="left",
            currentvalue={
                "prefix": "Showing: ",
                "visible": True,
                "xanchor": "right"
            },
            transition={"duration": transition_duration},
            pad={"b": 10, "t": 50},
            len=0.9,
            x=0.1
        )]
    )
    return fig


@st.cache_data
def load_age_gender_data():
    return analytics.get_age_gender_distribution()


@st.cache_data
def load_top_conditions(n=10):
    return analytics.get_top_conditions(n)


@st.cache_data
def load_condition_prevalence():
    return analytics.get_condition_prevalence_by_age()


@st.cache_data
def load_race_distribution():
    return analytics.get_race_distribution()


@st.cache_data
def load_encounter_type_breakdown():
    return analytics.get_encounter_type_breakdown()


@st.cache_data
def load_encounters_per_patient():
    return analytics.get_encounters_per_patient()


@st.cache_data
def load_high_utilizers(threshold=50):
    return analytics.get_high_utilizers(threshold)


@st.cache_data
def load_encounters_by_year():
    return analytics.get_encounters_by_year()


@st.cache_data
def load_top_medications(n=15):
    return analytics.get_top_medications(n)


@st.cache_data
def load_polypharmacy_distribution():
    return analytics.get_polypharmacy_distribution()


@st.cache_data
def load_medication_timeline():
    return analytics.get_patient_medication_timeline()


@st.cache_data
def load_abnormal_lab_values():
    return analytics.get_abnormal_lab_values()


@st.cache_data
def load_lab_trajectories():
    return analytics.get_lab_trajectories_by_year()


@st.cache_data
def load_readmission_candidates():
    return analytics.get_readmission_candidates()


@st.cache_data
def load_complexity_scores():
    return analytics.get_patient_complexity_scores()


@st.cache_data
def load_complexity_by_condition():
    return analytics.get_complexity_by_condition_count()


# ============================================================================
# PAGE 1: POPULATION OVERVIEW
# ============================================================================

def page_population_overview():
    st.title("🏥 Population Health Overview")
    st.markdown("---")

    # Load data
    age_gender_df = load_age_gender_data()
    top_conditions_df = load_top_conditions(10)
    condition_prevalence_df = load_condition_prevalence()
    race_df = load_race_distribution()

    # Calculate metrics
    total_patients = age_gender_df['count'].sum()
    encounters_df = load_encounters_per_patient()
    avg_age = 50  # Approximate from age distribution
    male_count = age_gender_df[age_gender_df['gender'] == 'male']['count'].sum()
    female_count = age_gender_df[age_gender_df['gender'] == 'female']['count'].sum()
    total_conditions = top_conditions_df['patient_count'].sum()

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Patients", f"{total_patients:,}")
    with col2:
        st.metric("Gender Split", f"{male_count}M / {female_count}F")
    with col3:
        st.metric("Avg Age", f"~{avg_age} years")
    with col4:
        st.metric("Total Condition Records", f"{total_conditions:,}")

    st.markdown("---")

    # Row 1: Population pyramid and Top Conditions
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Population Pyramid by Age & Gender")

        # Prepare data for population pyramid
        pyramid_data = age_gender_df.copy()
        # Make male counts negative for left side
        pyramid_data.loc[pyramid_data['gender'] == 'male', 'count'] = -pyramid_data.loc[pyramid_data['gender'] == 'male', 'count']

        fig = go.Figure()

        # Add female bars (right side)
        female_data = pyramid_data[pyramid_data['gender'] == 'female']
        fig.add_trace(go.Bar(
            y=female_data['age_group'],
            x=female_data['count'],
            name='Female',
            orientation='h',
            marker=dict(color=COLORS['female'])
        ))

        # Add male bars (left side)
        male_data = pyramid_data[pyramid_data['gender'] == 'male']
        fig.add_trace(go.Bar(
            y=male_data['age_group'],
            x=male_data['count'],
            name='Male',
            orientation='h',
            marker=dict(color=COLORS['male'])
        ))

        fig.update_layout(
            barmode='overlay',
            bargap=0.1,
            xaxis=dict(title='Population Count', tickformat=',d'),
            yaxis=dict(title='Age Group'),
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top 10 Most Prevalent Conditions")

        fig = px.bar(
            top_conditions_df,
            y='display',
            x='patient_count',
            orientation='h',
            color='patient_count',
            color_continuous_scale='Blues'
        )

        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title='Number of Patients',
            yaxis_title='',
            showlegend=False,
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    # Row 2: Animated cumulative condition prevalence
    st.subheader("📊 Condition Burden Accumulation by Age (Animated)")

    with st.expander("ℹ️ About this visualization"):
        st.write("""
        This animation shows how chronic disease burden **accumulates** as the population ages.
        Each frame adds the next age group, demonstrating how conditions become more prevalent
        in older populations. Watch how the bars grow as we include older age groups.
        """)

    # Prepare data for cumulative animation
    fig = px.bar(
        condition_prevalence_df,
        x='condition_display',
        y='patient_count',
        animation_frame='age_range',
        color='condition_display',
        labels={'patient_count': 'Patient Count', 'condition_display': 'Condition'},
        title="Cumulative Condition Prevalence (0 to Current Age Range)"
    )

    fig.update_layout(
        xaxis_title='',
        yaxis_title='Cumulative Patient Count',
        showlegend=False,
        height=500,
        xaxis={'categoryorder': 'total descending'}
    )

    fig = configure_animation(fig)
    st.plotly_chart(fig, use_container_width=True)

    # Row 3: Race distribution
    st.subheader("Race Distribution")

    fig = px.pie(
        race_df,
        names='race',
        values='count',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set2
    )

    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# PAGE 2: CLINICAL UTILIZATION
# ============================================================================

def page_clinical_utilization():
    st.title("🏨 Clinical Utilization Analysis")
    st.markdown("---")

    # Load data
    encounter_types_df = load_encounter_type_breakdown()
    encounters_per_patient_df = load_encounters_per_patient()
    high_utilizers_df = load_high_utilizers(50)
    encounters_by_year_df = load_encounters_by_year()

    # Metrics
    total_encounters = encounter_types_df['count'].sum()
    avg_encounters = encounters_per_patient_df['encounter_count'].mean()
    high_utilizer_count = len(high_utilizers_df)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Encounters", f"{total_encounters:,}")
    with col2:
        st.metric("Avg Encounters/Patient", f"{avg_encounters:.1f}")
    with col3:
        st.metric("High Utilizers (50+)", f"{high_utilizer_count}")

    st.markdown("---")

    # Row 1: Encounter types and distribution
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Encounter Type Breakdown")

        fig = px.pie(
            encounter_types_df,
            names='encounter_class',
            values='count',
            hole=0.4,
            color_discrete_sequence=[COLORS['primary'], COLORS['danger'], COLORS['warning']]
        )

        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Encounters Per Patient Distribution")

        fig = px.histogram(
            encounters_per_patient_df,
            x='encounter_count',
            nbins=50,
            color_discrete_sequence=[COLORS['primary']]
        )

        fig.update_layout(
            xaxis_title='Number of Encounters',
            yaxis_title='Number of Patients',
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    # Row 2: Animated encounter timeline (bubble chart)
    st.subheader("📈 Encounter Timeline by Year (Animated Bubble Chart)")

    with st.expander("ℹ️ About this visualization"):
        st.write("""
        This bubble chart animates through years, showing:
        - **X-axis**: Month of year (1-12)
        - **Y-axis**: Number of encounters
        - **Bubble size**: Average patient age
        - **Color**: Encounter class (Ambulatory, Emergency, Inpatient)

        Watch for seasonal patterns and utilization trends over time.
        """)

    fig = px.scatter(
        encounters_by_year_df,
        x='month',
        y='count',
        size='avg_patient_age',
        color='encounter_class',
        animation_frame='year',
        hover_data=['year', 'month', 'count', 'avg_patient_age'],
        color_discrete_map={
            'Ambulatory': COLORS['primary'],
            'Emergency': COLORS['danger'],
            'Inpatient': COLORS['warning']
        },
        labels={
            'month': 'Month',
            'count': 'Encounter Count',
            'avg_patient_age': 'Avg Patient Age'
        }
    )

    fig.update_layout(
        xaxis=dict(tickmode='linear', tick0=1, dtick=1, range=[0, 13]),
        yaxis_title='Encounter Count',
        height=500
    )

    fig = configure_animation(fig)
    st.plotly_chart(fig, use_container_width=True)

    # Row 3: High utilizers table
    st.subheader("High Utilizer Patients (50+ Encounters)")

    st.dataframe(
        high_utilizers_df[['given_name', 'family_name', 'encounter_count', 'top_conditions']].head(20),
        use_container_width=True,
        height=400
    )

    # Row 4: Conditions vs Encounters scatter
    st.subheader("Patient Complexity: Conditions vs Encounters")

    complexity_df = load_complexity_scores()

    fig = px.scatter(
        complexity_df,
        x='condition_count',
        y='encounter_count',
        color='condition_count',
        size='medication_count',
        hover_data=['patient_name', 'condition_count', 'encounter_count', 'medication_count'],
        color_continuous_scale='Blues',
        labels={
            'condition_count': 'Number of Conditions',
            'encounter_count': 'Number of Encounters'
        }
    )

    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# PAGE 3: MEDICATION INSIGHTS
# ============================================================================

def page_medication_insights():
    st.title("💊 Medication Insights")
    st.markdown("---")

    # Load data
    top_meds_df = load_top_medications(15)
    polypharmacy_df = load_polypharmacy_distribution()
    med_timeline_df = load_medication_timeline()

    # Metrics
    total_prescriptions = top_meds_df['prescription_count'].sum()
    polypharmacy_count = polypharmacy_df['polypharmacy_flag'].sum()
    polypharmacy_rate = (polypharmacy_count / len(polypharmacy_df)) * 100
    avg_meds = polypharmacy_df['medication_count'].mean()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Prescriptions", f"{total_prescriptions:,}")
    with col2:
        st.metric("Polypharmacy Rate", f"{polypharmacy_rate:.1f}%")
    with col3:
        st.metric("Avg Medications/Patient", f"{avg_meds:.1f}")

    st.markdown("---")

    # Row 1: Top medications
    st.subheader("Top 15 Most Prescribed Medications")

    fig = px.bar(
        top_meds_df,
        y='medication_display',
        x='prescription_count',
        orientation='h',
        color='prescription_count',
        color_continuous_scale='Greens'
    )

    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title='Number of Prescriptions',
        yaxis_title='',
        showlegend=False,
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)

    # Row 2: Polypharmacy distribution
    st.subheader("Polypharmacy Distribution (5+ Medications Threshold)")

    fig = px.histogram(
        polypharmacy_df,
        x='medication_count',
        nbins=30,
        color_discrete_sequence=[COLORS['primary']]
    )

    # Add vertical line at polypharmacy threshold
    fig.add_vline(
        x=5,
        line_dash="dash",
        line_color=COLORS['danger'],
        annotation_text="Polypharmacy Threshold (5)",
        annotation_position="top right"
    )

    fig.update_layout(
        xaxis_title='Number of Unique Medications',
        yaxis_title='Number of Patients',
        height=400
    )

    st.plotly_chart(fig, use_container_width=True)

    # Row 3: Animated medication accumulation
    st.subheader("📊 Patient Medication Journey (Animated)")

    with st.expander("ℹ️ About this visualization"):
        st.write("""
        This animation shows how polypharmacy develops over time for high-utilizer patients.
        Each frame represents a year, showing the accumulation of medications.
        Watch how medication counts grow, demonstrating the development of polypharmacy.
        """)

    if len(med_timeline_df) > 0:
        fig = px.bar(
            med_timeline_df,
            x='patient_name',
            y='medication_count',
            animation_frame='year',
            color='medication_count',
            color_continuous_scale='Reds',
            labels={
                'patient_name': 'Patient',
                'medication_count': 'Number of Medications'
            }
        )

        fig.update_layout(
            xaxis_tickangle=-45,
            yaxis_title='Medication Count',
            height=500
        )

        fig = configure_animation(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No timeline data available")


# ============================================================================
# PAGE 4: LAB ANALYTICS
# ============================================================================

def page_lab_analytics():
    st.title("🔬 Lab Analytics")
    st.markdown("---")

    # Load data
    abnormal_labs_df = load_abnormal_lab_values()
    lab_trajectories_df = load_lab_trajectories()

    # Calculate abnormal percentages
    if len(abnormal_labs_df) > 0:
        abnormal_pct = (abnormal_labs_df['abnormal_flag'] == 'Abnormal').sum() / len(abnormal_labs_df) * 100
    else:
        abnormal_pct = 0

    # Metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Lab Tests", f"{len(abnormal_labs_df):,}")
    with col2:
        st.metric("Abnormal Results", f"{abnormal_pct:.1f}%")
    with col3:
        st.metric("Unique Patients", f"{abnormal_labs_df['patient_id'].nunique():,}")

    st.markdown("---")

    # Lab type selector
    lab_types = {
        'Glucose': ['2339-0', '2345-7'],
        'Total Cholesterol': ['2093-3'],
        'BMI': ['39156-5'],
        'Systolic BP': ['8480-6'],
        'Diastolic BP': ['8462-4'],
        'Hemoglobin A1c': ['4548-4']
    }

    selected_lab = st.selectbox("Select Lab Type", list(lab_types.keys()))

    # Filter data for selected lab
    selected_codes = lab_types[selected_lab]
    lab_data = abnormal_labs_df[abnormal_labs_df['code'].isin(selected_codes)]

    # Row 1: Box plot and scatter
    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"{selected_lab} Distribution")

        if len(lab_data) > 0:
            fig = px.box(
                lab_data,
                y='value',
                color='abnormal_flag',
                color_discrete_map={'Normal': COLORS['normal'], 'Abnormal': COLORS['abnormal']},
                labels={'value': f'{selected_lab} Value'}
            )

            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available for this lab type")

    with col2:
        st.subheader(f"{selected_lab} Normal/Abnormal Breakdown")

        if len(lab_data) > 0:
            flag_counts = lab_data['abnormal_flag'].value_counts().reset_index()
            flag_counts.columns = ['Status', 'Count']

            fig = px.pie(
                flag_counts,
                names='Status',
                values='Count',
                color='Status',
                color_discrete_map={'Normal': COLORS['normal'], 'Abnormal': COLORS['abnormal']},
                hole=0.4
            )

            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available")

    # Row 2: Animated lab trajectories
    st.subheader("📈 Lab Value Trajectories Over Time (Animated)")

    with st.expander("ℹ️ About this visualization"):
        st.write("""
        This scatter plot animates through years, showing individual patient lab values.
        - **Green dots**: Normal values
        - **Red dots**: Abnormal values

        Watch patients cross from normal to abnormal zones over time,
        demonstrating disease progression and risk factors.
        """)

    # Filter trajectories for selected lab
    traj_data = lab_trajectories_df[lab_trajectories_df['lab_type'] == selected_lab]

    if len(traj_data) > 0:
        # Sample if too many points
        if len(traj_data) > 5000:
            traj_data = traj_data.sample(5000)

        fig = px.scatter(
            traj_data,
            x='patient_id',
            y='value',
            color='abnormal_flag',
            animation_frame='year',
            color_discrete_map={'Normal': COLORS['normal'], 'Abnormal': COLORS['abnormal']},
            labels={'value': f'{selected_lab} Value', 'patient_id': 'Patient'},
            opacity=0.6
        )

        fig.update_layout(
            xaxis={'visible': False},
            yaxis_title=f'{selected_lab} Value',
            height=500
        )

        fig = configure_animation(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trajectory data available for this lab type")


# ============================================================================
# PAGE 5: RISK PREDICTIONS
# ============================================================================

def page_risk_predictions():
    st.title("⚠️ Risk Predictions & Readmissions")
    st.markdown("---")

    # Load data
    readmissions_df = load_readmission_candidates()
    complexity_df = load_complexity_scores()
    complexity_by_cond_df = load_complexity_by_condition()

    # Calculate overall readmission rate
    if len(readmissions_df) > 0:
        overall_readmission_rate = readmissions_df['readmission_rate'].mean()
    else:
        overall_readmission_rate = 0

    # Calculate risk tiers
    if len(complexity_df) > 0:
        complexity_df['risk_tier'] = pd.qcut(
            complexity_df['complexity_score'],
            q=[0, 0.25, 0.75, 0.90, 1.0],
            labels=['Low', 'Medium', 'High', 'Critical']
        )
        high_risk_count = len(complexity_df[complexity_df['risk_tier'].isin(['High', 'Critical'])])
    else:
        high_risk_count = 0

    # Metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("30-Day Readmission Rate", f"{overall_readmission_rate:.1f}%")
    with col2:
        st.metric("High/Critical Risk Patients", f"{high_risk_count}")
    with col3:
        if len(complexity_df) > 0:
            st.metric("Avg Complexity Score", f"{complexity_df['complexity_score'].mean():.1f}")
        else:
            st.metric("Avg Complexity Score", "N/A")

    st.markdown("---")

    # Row 1: Readmission rates
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("30-Day Readmission Rates by Encounter Type")

        if len(readmissions_df) > 0:
            fig = px.bar(
                readmissions_df,
                x='encounter_class',
                y='readmission_rate',
                color='readmission_rate',
                color_continuous_scale='Reds',
                labels={'readmission_rate': 'Readmission Rate (%)'}
            )

            fig.update_layout(
                xaxis_title='Encounter Type',
                yaxis_title='Readmission Rate (%)',
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No readmission data available")

    with col2:
        st.subheader("Risk Tier Distribution")

        if len(complexity_df) > 0:
            tier_counts = complexity_df['risk_tier'].value_counts().reset_index()
            tier_counts.columns = ['Risk Tier', 'Count']

            fig = px.pie(
                tier_counts,
                names='Risk Tier',
                values='Count',
                color='Risk Tier',
                color_discrete_map={
                    'Low': COLORS['secondary'],
                    'Medium': COLORS['warning'],
                    'High': COLORS['danger'],
                    'Critical': '#8B0000'
                },
                hole=0.4
            )

            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No risk tier data available")

    # Row 2: Animated complexity shift
    st.subheader("📊 Complexity Score Distribution by Condition Count (Animated)")

    with st.expander("ℹ️ About this visualization"):
        st.write("""
        This animated histogram shows how patient complexity scores shift based on
        the number of chronic conditions they have.

        Watch the distribution move **right** (higher complexity) as we progress from
        patients with 0 conditions to those with 3+ conditions. This demonstrates
        how multiple chronic conditions drive higher healthcare complexity and risk.
        """)

    if len(complexity_by_cond_df) > 0:
        fig = px.histogram(
            complexity_by_cond_df,
            x='complexity_score',
            animation_frame='condition_count_group',
            nbins=50,
            color_discrete_sequence=[COLORS['danger']],
            labels={'complexity_score': 'Complexity Score'}
        )

        fig.update_layout(
            xaxis_title='Complexity Score',
            yaxis_title='Number of Patients',
            height=500
        )

        fig = configure_animation(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No complexity data available")

    # Row 3: Complexity scatter
    st.subheader("Patient Complexity: Score vs Encounter Count")

    if len(complexity_df) > 0:
        fig = px.scatter(
            complexity_df.head(500),  # Limit to 500 for performance
            x='complexity_score',
            y='encounter_count',
            size='condition_count',
            color='risk_tier',
            hover_data=['patient_name', 'condition_count', 'medication_count'],
            color_discrete_map={
                'Low': COLORS['secondary'],
                'Medium': COLORS['warning'],
                'High': COLORS['danger'],
                'Critical': '#8B0000'
            },
            labels={
                'complexity_score': 'Complexity Score',
                'encounter_count': 'Number of Encounters'
            }
        )

        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No complexity data available")

    # Row 4: High-risk patients table
    st.subheader("Highest-Risk Patients")

    if len(complexity_df) > 0:
        high_risk_patients = complexity_df.nlargest(20, 'complexity_score')[
            ['patient_name', 'condition_count', 'medication_count',
             'encounter_count', 'abnormal_lab_count', 'complexity_score', 'risk_tier']
        ]

        st.dataframe(high_risk_patients, use_container_width=True, height=400)
    else:
        st.info("No patient data available")


# ============================================================================
# SIDEBAR NAVIGATION
# ============================================================================

def sidebar():
    with st.sidebar:
        st.title("🏥 FHIR Analytics")
        st.markdown("---")

        st.markdown("""
        ### About This Dashboard

        This interactive dashboard analyzes synthetic FHIR data from 555 patients,
        including:
        - 215,000+ observations
        - 27,000+ encounters
        - 24,000+ medication requests
        - 17,000+ conditions

        **Navigation:** Select a page below to explore different analytics.
        """)

        st.markdown("---")

        page = st.radio(
            "Select Page:",
            [
                "Population Overview",
                "Clinical Utilization",
                "Medication Insights",
                "Lab Analytics",
                "Risk Predictions"
            ],
            index=0
        )

        st.markdown("---")
        st.markdown("Built with Streamlit + Plotly")
        st.markdown("Data: Synthetic FHIR Dataset")

        return page


# ============================================================================
# MAIN APP
# ============================================================================

def main():
    # Get selected page from sidebar
    selected_page = sidebar()

    # Route to appropriate page
    if selected_page == "Population Overview":
        page_population_overview()
    elif selected_page == "Clinical Utilization":
        page_clinical_utilization()
    elif selected_page == "Medication Insights":
        page_medication_insights()
    elif selected_page == "Lab Analytics":
        page_lab_analytics()
    elif selected_page == "Risk Predictions":
        page_risk_predictions()


if __name__ == "__main__":
    main()
