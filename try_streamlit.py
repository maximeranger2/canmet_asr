import streamlit as st
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Streamlit app config
st.set_page_config(page_title="CANMET DB", layout="wide")
st.title("CANMET Field Exposure Site Database")

# --- Sidebar for login ---
st.sidebar.header("Authentication")
username = st.sidebar.text_input("Username")
password = st.sidebar.text_input("Password", type="password")

# Initialize connection
@st.cache_resource
def get_connection(user, pwd):
    try:
        return psycopg2.connect(
            dbname="ul_val_prj_canmet_asr",
            user=user,
            password=pwd,
            host="postgres-5.svc.valeria.science"
        )
    except:
        return None

if username and password:
    conn = get_connection(username, password)
    if conn:
        st.sidebar.success("Connected to database")

        # Fetch type list
        @st.cache_data
        def fetch_types():
            q = '''SELECT t.id, n.details
                   FROM public.canmet_site_material_description t
                   JOIN public.canmet_site_material_description n ON t.id = n.id
                   WHERE t.info = 'type' AND n.info = 'name' AND t.details = 'Aggregate';'''
            df = pd.read_sql(q, conn)
            return dict(zip(df['details'], df['id']))

        type_dict = fetch_types()

        # --- Query Filters ---
        st.header("Select Filters")
        data_type = st.selectbox("Data type", ["field", "lab"])
        selected_types = st.multiselect("Reactive aggregate", list(type_dict.keys()))
        binder_map = {
            "PC": 'None',
            "PC + silica fume": "SF",
            "PC + fly ash": "FA",
            "PC + slag": "SG",
            "PC + fly ash + silica fume": "FA+SF",
        }
        selected_binders = st.multiselect("Binder type", list(binder_map.keys()))
        selected_boosting = st.multiselect("Boosting level", ["Not boosted", "Boosted"])
        selected_lithium = st.multiselect("Lithium level", ["No lithium", "LiOH", "LiNO3"])

        last_options = []
        if data_type == "field":
            last_options = st.multiselect("Element type", ["Blocks (A and B)", "Slab (S)"])
        else:
            last_options = st.multiselect("Lab test", ["CPT38-RH95", "CPT38-NaOH1N", "CPT80-NaOH1N", "CPT60-RH95",
                                                       "CPT38-NaCl5%", "AMBT"])

        if st.button("Run Query"):
            # Build query
            try:
                aggregate_conditions = " OR ".join(["m.type = %s"] * len(selected_types))
                binder_conditions = []
                for prefix in [binder_map[b] for b in selected_binders]:
                    if prefix == 'None':
                        binder_conditions.append("m_binder.type = 'PC'")
                    else:
                        binder_conditions.append(f"m_binder.type = 'PC+{prefix}'")
                boosting_conditions = []
                if "Not boosted" in selected_boosting:
                    boosting_conditions.append("(md.component = 'naoh' AND md.amount = 0)")
                if "Boosted" in selected_boosting:
                    boosting_conditions.append("(md.component = 'naoh' AND md.amount != 0)")
                lithium_conditions = []
                if "No lithium" in selected_lithium:
                    lithium_conditions.append("NOT EXISTS (SELECT 1 FROM public.canmet_site_mix_design md2 WHERE md2.id = md.id AND md2.component IN ('lioh', 'ltn') AND md2.amount != 0)")
                if "LiOH" in selected_lithium:
                    lithium_conditions.append("EXISTS (SELECT 1 FROM public.canmet_site_mix_design md2 WHERE md2.id = md.id AND md2.component = 'lioh' AND md2.amount != 0)")
                if "LiNO3" in selected_lithium:
                    lithium_conditions.append("EXISTS (SELECT 1 FROM public.canmet_site_mix_design md2 WHERE md2.id = md.id AND md2.component = 'ltn' AND md2.amount != 0)")

                params = [type_dict[k] for k in selected_types]

                if data_type == "field":
                    type_cond = []
                    if "Blocks (A and B)" in last_options:
                        type_cond.append("e.block IN ('A', 'B', 'AB')")
                    if "Slab (S)" in last_options:
                        type_cond.append("e.block = 'S'")
                    query = f"""
                    SELECT e.id, fn.designation, e.date, e.age, e.block, e.position, e.expansion
                    FROM public.canmet_site_expansion e
                    JOIN public.canmet_site_mix_materials m ON e.id = m.id
                    JOIN public.canmet_site_mix_materials m_binder ON e.id = m_binder.id AND m_binder.material = 'binder'
                    JOIN public.canmet_site_full_names fn ON e.id = fn.id
                    JOIN public.canmet_site_mix_design md ON e.id = md.id
                    WHERE m.material = 'coarse aggregate'
                    AND ({aggregate_conditions})
                    AND ({' OR '.join(binder_conditions)})
                    AND ({' OR '.join(type_cond)})
                    AND ({' OR '.join(boosting_conditions)})
                    AND ({' OR '.join(lithium_conditions)});
                    """
                else:
                    test_conditions = " OR ".join(["l.test = %s"] * len(last_options))
                    params += last_options
                    query = f"""
                    SELECT l.id, fn.designation, l.test, l.age, l.expansion
                    FROM public.canmet_site_lab_tests l
                    JOIN public.canmet_site_mix_materials m ON l.id = m.id
                    JOIN public.canmet_site_mix_materials m_binder ON l.id = m_binder.id AND m_binder.material = 'binder'
                    JOIN public.canmet_site_full_names fn ON l.id = fn.id
                    JOIN public.canmet_site_mix_design md ON l.id = md.id
                    WHERE m.material = 'coarse aggregate'
                    AND ({aggregate_conditions})
                    AND ({' OR '.join(binder_conditions)})
                    AND ({test_conditions})
                    AND ({' OR '.join(boosting_conditions)})
                    AND ({' OR '.join(lithium_conditions)});
                    """

                df = pd.read_sql(query, conn, params=params)
                st.success(f"{len(df)} rows returned.")
                st.dataframe(df)

                # Plotting
                if not df.empty:
                    st.header("Expansion Plot")
                    fig, ax = plt.subplots(figsize=(10, 6))
                    if data_type == "field":
                        for id_, group in df.groupby("id"):
                            ax.plot(group["age"], group["expansion"], marker='o', label=f"{id_}")
                        ax.set_xlabel("Age [year]")
                    else:
                        for id_, group in df.groupby("id"):
                            ax.plot(group["age"], group["expansion"], marker='o', label=f"{id_}")
                        ax.set_xlabel("Test duration [weeks/days]")
                    ax.set_ylabel("Expansion [%]")
                    ax.legend()
                    st.pyplot(fig)
            except Exception as e:
                st.error(f"Error: {e}")
    else:
        st.sidebar.error("Failed to connect to database")
else:
    st.info("Please enter credentials to continue")
