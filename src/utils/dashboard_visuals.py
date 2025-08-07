"""
Utilities for generating visualizations for the dashboard.
"""
import pandas as pd
import plotly.graph_objects as go

def create_sankey_diagram(df: pd.DataFrame, as_html: bool = False):
    """
    Creates a Sankey diagram from the job data showing the flow from
    Job Category -> Team -> Role.

    Args:
        df: A pandas DataFrame containing job data.
        as_html: If True, returns the chart as a self-contained HTML string.
                 If False, returns the Plotly Figure object.

    Returns:
        A Plotly Figure object or an HTML string of the figure.
    """
    # Filter the DataFrame to include only active jobs
    df_active = df[df['active'] == True].copy()
    
    if df_active.empty:
        if as_html:
            return "<h4>No active jobs to display in the Sankey diagram.</h4>"
        return go.Figure()

    # Data Preparation: Create links for the Sankey diagram
    
    # 1. Flow from Job Category to Team
    category_to_team = df_active.groupby(['job_category', 'team']).size().reset_index(name='value')
    
    # 2. Flow from Team to Role
    team_to_role = df_active.groupby(['team', 'role']).size().reset_index(name='value')

    # Combine the data for both flows
    flow_data = pd.DataFrame(
        columns=['source', 'target', 'value'],
        data=pd.concat([
            category_to_team.rename(columns={'job_category': 'source', 'team': 'target'}),
            team_to_role.rename(columns={'team': 'source', 'role': 'target'})
        ])
    )
    
    # Create a list of all unique nodes (categories, teams, and roles)
    all_nodes = pd.Series(
        pd.concat([flow_data['source'], flow_data['target']]).unique()
    ).reset_index().rename(columns={'index': 'node_id', 0: 'name'})

    # Map node names to their IDs for the Sankey links
    flow_data = pd.merge(flow_data, all_nodes, left_on='source', right_on='name', how='left').rename(columns={'node_id': 'source_id'})
    flow_data = pd.merge(flow_data, all_nodes, left_on='target', right_on='name', how='left').rename(columns={'node_id': 'target_id'})
    
    # --- NEW CORRECTED COUNT LOGIC ---
    
    # Calculate the total job count for each unique category, team, and role
    category_counts = df_active.groupby('job_category').size()
    team_counts = df_active.groupby('team').size()
    role_counts = df_active.groupby('role').size()
    
    # Combine the counts into a single Series
    all_counts = pd.concat([category_counts, team_counts, role_counts])
    
    # Merge the correct counts into the all_nodes DataFrame
    all_nodes['count'] = all_nodes['name'].map(all_counts)
    all_nodes['count'] = all_nodes['count'].fillna(0).astype(int)
    
    # Format the node labels to include the correct total count
    node_labels = [f"{name} ({count})" for name, count in all_nodes[['name', 'count']].values]
    
    # --- END NEW CORRECTED COUNT LOGIC ---
    
    # Create the Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=node_labels, # Use the new, correct labels with counts
            hoverinfo='all'
        ),
        link=dict(
            source=flow_data['source_id'],
            target=flow_data['target_id'],
            value=flow_data['value']
        )
    )])

    fig.update_layout(
        title_text="Job Distribution Flow: Category → Team → Role",
        font_size=12,
        height=600,
        margin=dict(l=10, r=10, t=50, b=10)
    )
    
    if as_html:
        return fig.to_html(full_html=False, include_plotlyjs='cdn')
    else:
        return fig