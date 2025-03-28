#!/usr/bin/env julia

"""
PipeDuck Node: Analyze Sales Data
This node performs advanced analysis on the processed sales data.
"""

# Install and load required packages
using Pkg

# List of required packages
required_packages = [
    "DataFrames",
    "Dates",
    "Statistics",
    "JSON"
]

# Add and use PipeDuck
using PipeDuck
using DuckConnect

# Install missing packages
for pkg in required_packages
    try
        @eval using $Symbol(pkg)
    catch
        println("Installing $pkg...")
        Pkg.add(pkg)
        @eval using $Symbol(pkg)
    end
end

function calculate_moving_average(df, date_col, value_col, window)
    """Calculate moving average for a DataFrame column"""
    sorted_df = sort(df, date_col)
    result = copy(sorted_df)
    result[!, "$(value_col)_ma$(window)"] = zeros(Float64, size(sorted_df, 1))
    
    for i in 1:size(sorted_df, 1)
        start_idx = max(1, i - window + 1)
        result[i, "$(value_col)_ma$(window)"] = mean(sorted_df[start_idx:i, value_col])
    end
    
    return result
end

function normalize(x)
    """Normalize values between 0 and 1"""
    min_x, max_x = extrema(x)
    return (max_x == min_x) ? fill(0.5, length(x)) : (x .- min_x) ./ (max_x - min_x)
end

function generate_report(daily_sales, store_performance, output_path)
    """Generate a text report summarizing the analysis"""
    open(output_path, "w") do f
        write(f, "# Sales Analysis Report\n\n")
        write(f, "Generated on: $(Dates.now())\n\n")
        
        write(f, "## Overview\n\n")
        write(f, "Total days analyzed: $(size(daily_sales, 1))\n")
        write(f, "Total stores analyzed: $(size(store_performance, 1))\n")
        write(f, "Total revenue: \$$(round(sum(daily_sales.total_revenue), digits=2))\n")
        write(f, "Total quantity sold: $(sum(daily_sales.total_quantity))\n\n")
        
        write(f, "## Top 5 Performing Stores\n\n")
        for i in 1:min(5, size(store_performance, 1))
            store = store_performance[i, :]
            write(f, "$(i). Store ID: $(store.store_id)\n")
            write(f, "   Composite Score: $(round(store.composite_score * 100, digits=1))%\n")
            write(f, "   Total Revenue: \$$(round(store.total_revenue, digits=2))\n")
            write(f, "   Product Variety: $(store.product_variety)\n\n")
        end
        
        write(f, "## Sales Trends\n\n")
        write(f, "Average daily revenue: \$$(round(mean(daily_sales.total_revenue), digits=2))\n")
        write(f, "Average transaction size: \$$(round(mean(daily_sales.total_revenue ./ daily_sales.transaction_count), digits=2))\n")
        write(f, "Average price per item: \$$(round(mean(daily_sales.avg_price), digits=2))\n")
        
        # Add summary of 7-day moving averages
        if hasproperty(daily_sales, :total_revenue_ma7)
            last_ma = daily_sales[end, :total_revenue_ma7]
            first_ma = daily_sales[min(7, size(daily_sales, 1)), :total_revenue_ma7]
            change = (last_ma - first_ma) / first_ma * 100
            
            write(f, "\n## Revenue Trend Analysis\n\n")
            write(f, "7-day moving average (start): \$$(round(first_ma, digits=2))\n")
            write(f, "7-day moving average (end): \$$(round(last_ma, digits=2))\n")
            write(f, "Change: $(round(change, digits=1))%\n")
        end
    end
end

function main()
    """Main function to execute when the node is run"""
    println("Executing node: Analyze Sales Data")
    
    # Get metadata path from command-line arguments (used by PipeDuck)
    meta_path = length(ARGS) > 0 ? ARGS[1] : nothing
    
    # Create DuckContext to manage database connections
    ctx = DuckContext(meta_path=meta_path)
    
    # Get the processed data from the previous node
    println("Loading processed data...")
    daily_sales = get_input(ctx, "daily_sales")
    store_performance = get_input(ctx, "store_performance")
    
    println("Loaded daily_sales ($(size(daily_sales, 1)) rows) and store_performance ($(size(store_performance, 1)) rows)")
    
    # Make sure date is in the right format
    if typeof(daily_sales.date) <: AbstractString
        daily_sales.date = Date.(daily_sales.date)
    end
    
    # 1. Calculate moving averages for daily sales
    println("Calculating 7-day moving averages...")
    daily_sales_with_ma = calculate_moving_average(daily_sales, :date, :total_revenue, 7)
    daily_sales_with_ma = calculate_moving_average(daily_sales_with_ma, :date, :total_quantity, 7)
    
    # 2. Store performance scoring
    println("Creating store performance scoring...")
    
    # Create a composite store score
    store_performance.revenue_score = normalize(store_performance.total_revenue)
    store_performance.quantity_score = normalize(store_performance.total_quantity)
    store_performance.variety_score = normalize(store_performance.product_variety)
    store_performance.efficiency_score = normalize(store_performance.avg_revenue_per_transaction)
    
    # Composite score (weighted)
    store_performance.composite_score = (
        0.4 * store_performance.revenue_score +
        0.3 * store_performance.quantity_score +
        0.2 * store_performance.variety_score +
        0.1 * store_performance.efficiency_score
    )
    
    # Sort by composite score
    store_performance = sort(store_performance, :composite_score, rev=true)
    
    # 3. Generate a text report
    println("Generating summary report...")
    mkpath("data")
    report_path = "data/analysis_report.txt"
    generate_report(daily_sales_with_ma, store_performance, report_path)
    
    # 4. Save results for downstream use
    set_output(ctx, daily_sales_with_ma, "daily_sales_enhanced")
    set_output(ctx, store_performance, "store_performance_scored")
    
    # Create a basic report data structure for potential dashboard use
    report_data = DataFrame(
        report_type = ["sales_analysis"],
        generated_at = [string(Dates.now())],
        total_days = [size(daily_sales, 1)],
        total_stores = [size(store_performance, 1)],
        total_revenue = [sum(daily_sales.total_revenue)],
        total_quantity = [sum(daily_sales.total_quantity)],
        avg_daily_revenue = [mean(daily_sales.total_revenue)],
        top_store_id = [store_performance[1, :store_id]],
        top_store_score = [store_performance[1, :composite_score]],
        report_path = [report_path]
    )
    
    set_output(ctx, report_data, "report")
    
    println("Node execution complete")
    
    return 0
end

# Run the main function
exit_code = main()
exit(exit_code) 