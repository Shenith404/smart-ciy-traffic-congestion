"""
Traffic Visualization Script
Generates Traffic Volume vs. Time of Day visualization
Required for: Analytic Report requirement
"""

import os
import psycopg2
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environments
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'smartcity_db',
    'user': 'admin',
    'password': 'password'
}

# Output directory for reports
REPORT_DIR = "./data/reports"
os.makedirs(REPORT_DIR, exist_ok=True)

def fetch_traffic_data():
    """Fetch traffic data from PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Query 1: Hourly traffic volume
        query_hourly = """
            SELECT 
                EXTRACT(HOUR FROM timestamp) as hour,
                sensor_id,
                SUM(vehicle_count) as total_vehicles
            FROM traffic_windows
            WHERE window_start >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY sensor_id, EXTRACT(HOUR FROM timestamp)
            ORDER BY sensor_id, hour
        """
        
        # Query 2: Peak traffic stats
        query_peaks = """
            SELECT 
                junction_id,
                peak_hour,
                max_vehicle_count,
                report_date
            FROM peak_traffic_stats
            ORDER BY report_date DESC, max_vehicle_count DESC
            LIMIT 10
        """
        
        df_hourly = pd.read_sql(query_hourly, conn)
        df_peaks = pd.read_sql(query_peaks, conn)
        
        conn.close()
        return df_hourly, df_peaks
    
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame(), pd.DataFrame()

def create_visualizations(df_hourly, df_peaks):
    """Create comprehensive traffic visualizations"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Set style
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (16, 10)
    
    # Create subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Smart City Traffic Analysis - Colombo', fontsize=16, fontweight='bold')
    
    # Plot 1: Traffic Volume vs Time of Day (Line Chart)
    if not df_hourly.empty:
        ax1 = axes[0, 0]
        for junction in df_hourly['sensor_id'].unique():
            junction_data = df_hourly[df_hourly['sensor_id'] == junction]
            ax1.plot(junction_data['hour'], junction_data['total_vehicles'], 
                    marker='o', label=junction, linewidth=2)
        
        ax1.set_xlabel('Hour of Day', fontsize=12)
        ax1.set_ylabel('Total Vehicles', fontsize=12)
        ax1.set_title('Traffic Volume vs. Time of Day (Last 7 Days)', fontsize=13, fontweight='bold')
        ax1.legend(title='Junction')
        ax1.grid(True, alpha=0.3)
        ax1.set_xticks(range(0, 24))
    
    # Plot 2: Average Traffic by Junction (Bar Chart)
    if not df_hourly.empty:
        ax2 = axes[0, 1]
        avg_by_junction = df_hourly.groupby('sensor_id')['total_vehicles'].mean().sort_values(ascending=False)
        colors = sns.color_palette("husl", len(avg_by_junction))
        avg_by_junction.plot(kind='bar', ax=ax2, color=colors)
        ax2.set_xlabel('Junction ID', fontsize=12)
        ax2.set_ylabel('Average Vehicles', fontsize=12)
        ax2.set_title('Average Traffic Volume by Junction', fontsize=13, fontweight='bold')
        ax2.tick_params(axis='x', rotation=0)
        ax2.grid(axis='y', alpha=0.3)
    
    # Plot 3: Peak Hours Heatmap
    if not df_hourly.empty:
        ax3 = axes[1, 0]
        pivot_data = df_hourly.pivot_table(
            values='total_vehicles', 
            index='sensor_id', 
            columns='hour', 
            aggfunc='sum',
            fill_value=0
        )
        sns.heatmap(pivot_data, annot=False, fmt='g', cmap='YlOrRd', 
                   ax=ax3, cbar_kws={'label': 'Vehicle Count'})
        ax3.set_xlabel('Hour of Day', fontsize=12)
        ax3.set_ylabel('Junction ID', fontsize=12)
        ax3.set_title('Traffic Intensity Heatmap (Hour vs Junction)', fontsize=13, fontweight='bold')
    
    # Plot 4: Peak Traffic Summary Table
    if not df_peaks.empty:
        ax4 = axes[1, 1]
        ax4.axis('tight')
        ax4.axis('off')
        
        # Format data for table
        table_data = df_peaks[['junction_id', 'peak_hour', 'max_vehicle_count', 'report_date']].head(8)
        table_data.columns = ['Junction', 'Peak Hour', 'Max Vehicles', 'Date']
        
        table = ax4.table(cellText=table_data.values,
                         colLabels=table_data.columns,
                         cellLoc='center',
                         loc='center',
                         colWidths=[0.2, 0.2, 0.3, 0.3])
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        # Style header
        for i in range(len(table_data.columns)):
            table[(0, i)].set_facecolor('#4CAF50')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        ax4.set_title('Peak Traffic Statistics (Recent Reports)', 
                     fontsize=13, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # Save figure
    output_path = os.path.join(REPORT_DIR, f"traffic_report_{timestamp}.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization saved: {output_path}")
    
    # Also save as latest
    latest_path = os.path.join(REPORT_DIR, "traffic_report_latest.png")
    plt.savefig(latest_path, dpi=300, bbox_inches='tight')
    print(f"✓ Latest report saved: {latest_path}")
    
    plt.close()

def generate_text_report(df_peaks):
    """Generate text-based report for logging"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    report_path = os.path.join(REPORT_DIR, f"report_{datetime.now().strftime('%Y%m%d')}.txt")
    
    with open(report_path, 'w') as f:
        f.write("="*70 + "\n")
        f.write("SMART CITY TRAFFIC MANAGEMENT REPORT - COLOMBO\n")
        f.write(f"Generated: {timestamp}\n")
        f.write("="*70 + "\n\n")
        
        if not df_peaks.empty:
            f.write("JUNCTIONS REQUIRING TRAFFIC POLICE INTERVENTION:\n")
            f.write("-"*70 + "\n")
            
            for idx, row in df_peaks.head(4).iterrows():
                f.write(f"\n{idx+1}. Junction: {row['junction_id']}\n")
                f.write(f"   Peak Hour: {int(row['peak_hour'])}:00\n")
                f.write(f"   Max Vehicles: {int(row['max_vehicle_count'])}\n")
                f.write(f"   Report Date: {row['report_date']}\n")
            
            f.write("\n" + "="*70 + "\n")
            f.write("RECOMMENDATION: Deploy traffic police at above junctions during peak hours\n")
            f.write("="*70 + "\n")
        else:
            f.write("No peak traffic data available yet.\n")
    
    print(f"✓ Text report saved: {report_path}")


def export_csv_report(df_hourly, df_peaks):
    """Export the analyzed report as CSV for submission."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = os.path.join(REPORT_DIR, f"traffic_report_{timestamp}.csv")
    latest_csv_path = os.path.join(REPORT_DIR, "traffic_report_latest.csv")

    if df_hourly.empty and df_peaks.empty:
        print("⚠ No data available to export as CSV.")
        return

    report_rows = []

    if not df_peaks.empty:
        for _, row in df_peaks.iterrows():
            report_rows.append({
                "report_type": "peak_traffic",
                "junction_id": row.get("junction_id"),
                "peak_hour": row.get("peak_hour"),
                "max_vehicle_count": row.get("max_vehicle_count"),
                "report_date": row.get("report_date"),
                "hour": None,
                "total_vehicles": None,
            })

    if not df_hourly.empty:
        for _, row in df_hourly.iterrows():
            report_rows.append({
                "report_type": "hourly_traffic",
                "junction_id": row.get("sensor_id"),
                "peak_hour": None,
                "max_vehicle_count": None,
                "report_date": None,
                "hour": row.get("hour"),
                "total_vehicles": row.get("total_vehicles"),
            })

    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(csv_path, index=False)
    report_df.to_csv(latest_csv_path, index=False)

    print(f"✓ CSV report saved: {csv_path}")
    print(f"✓ Latest CSV report saved: {latest_csv_path}")

def main():
    """Main execution function"""
    print("="*60)
    print("Traffic Visualization Generator")
    print("="*60)
    
    # Fetch data
    print("\n1. Fetching data from PostgreSQL...")
    df_hourly, df_peaks = fetch_traffic_data()
    
    if df_hourly.empty and df_peaks.empty:
        print("⚠ No data available for visualization.")
        print("Ensure the streaming and batch jobs have run.")
        return
    
    print(f"   - Hourly records: {len(df_hourly)}")
    print(f"   - Peak reports: {len(df_peaks)}")
    
    # Create visualizations
    print("\n2. Generating visualizations...")
    create_visualizations(df_hourly, df_peaks)
    
    # Generate text report
    print("\n3. Generating text report...")
    generate_text_report(df_peaks)

    # Export CSV report for submission
    print("\n4. Exporting CSV report...")
    export_csv_report(df_hourly, df_peaks)
    
    print("\n" + "="*60)
    print("✓ Visualization complete!")
    print(f"  Reports location: {os.path.abspath(REPORT_DIR)}")
    print("="*60)

if __name__ == "__main__":
    main()
