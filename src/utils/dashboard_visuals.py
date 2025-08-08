"""
Dashboard visualization utilities
"""

import plotly.graph_objects as go  # type: ignore
import pandas as pd


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
    df_active = df[df["active"]].copy()

    if df_active.empty:
        if as_html:
            return "<h4>No active jobs to display in the Sankey diagram.</h4>"
        return go.Figure()

    category_to_team = (
        df_active.groupby(["job_category", "team"]).size().reset_index(name="value")
    )
    team_to_role = df_active.groupby(["team", "role"]).size().reset_index(name="value")

    flow_data = pd.DataFrame(
        columns=["source", "target", "value"],
        data=pd.concat(
            [
                category_to_team.rename(
                    columns={"job_category": "source", "team": "target"}
                ),
                team_to_role.rename(columns={"team": "source", "role": "target"}),
            ]
        ),
    )

    all_nodes = (
        pd.Series(pd.concat([flow_data["source"], flow_data["target"]]).unique())
        .reset_index()
        .rename(columns={"index": "node_id", 0: "name"})
    )

    flow_data = pd.merge(
        flow_data, all_nodes, left_on="source", right_on="name", how="left"
    ).rename(columns={"node_id": "source_id"})
    flow_data = pd.merge(
        flow_data, all_nodes, left_on="target", right_on="name", how="left"
    ).rename(columns={"node_id": "target_id"})

    categories = df_active["job_category"].unique()

    # Fixed palette to match the style of the documentation example
    plotly_palette = [
        "rgba(255,0,255, 0.8)",  # magenta
        "rgba(255,165,0, 0.8)",  # orange
        "rgba(255,255,0, 0.8)",  # yellow
        "rgba(0,128,0, 0.8)",  # green
        "rgba(0,0,255, 0.8)",  # blue
        "rgba(128,0,128, 0.8)",  # purple
        "rgba(0,255,255, 0.8)",  # cyan
    ]
    category_colors = {
        cat: plotly_palette[i % len(plotly_palette)] for i, cat in enumerate(categories)
    }

    node_color_map = {}
    for _, row in df_active.iterrows():
        category = row["job_category"]
        team = row["team"]
        role = row["role"]

        if category in category_colors:
            color = category_colors[category]
        else:
            color = "#CCCCCC"

        node_color_map[category] = color
        node_color_map[team] = color
        node_color_map[role] = color

    node_colors = [node_color_map.get(name, "#CCCCCC") for name in all_nodes["name"]]

    # MODIFIED: Change opacity to 0.4 to match the example
    opacity = 0.4
    link_colors = [
        node_colors[src_id].replace("0.8", str(opacity))
        for src_id in flow_data["source_id"]
    ]

    category_counts = df_active.groupby("job_category").size()
    team_counts = df_active.groupby("team").size()
    role_counts = df_active.groupby("role").size()

    all_counts = pd.concat([category_counts, team_counts, role_counts])
    all_nodes["count"] = all_nodes["name"].map(all_counts)
    all_nodes["count"] = all_nodes["count"].fillna(0).astype(int)

    node_labels = [
        f"{name} - {count}" for name, count in all_nodes[["name", "count"]].values
    ]

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=15,
                    line=dict(color="black", width=0.5),
                    color=node_colors,
                    label=node_labels,
                    hovertemplate="%{label}<extra></extra>",
                ),
                link=dict(
                    source=flow_data["source_id"],
                    target=flow_data["target_id"],
                    value=flow_data["value"],
                    color=link_colors,
                ),
            )
        ]
    )

    fig.update_layout(
        title_text="Amazon Luxembourg Job Flow: Category → Team → Role",
        font_size=10,
        height=600,
        margin=dict(l=10, r=10, t=50, b=10),
    )

    if as_html:
        return fig.to_html(full_html=False, include_plotlyjs="cdn")
    else:
        return fig
