"""Read live 13f-positions.json + verify AUMs per fund."""
import json
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("verify_13f_aum_real") as r:
        r.heading("Verify 13F AUMs are realistic now")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/13f-positions.json")
            data = json.loads(obj["Body"].read())
        except Exception as e:
            r.fail(f"  {e}")
            return

        r.log(f"  generated_at: {data.get('generated_at')}")
        r.log(f"  funds_parsed: {data.get('funds_parsed')} / {data.get('funds_total')}")
        
        funds = data.get("by_fund", {})
        r.section(f"Per-fund AUM ({len(funds)} funds)")
        # Sort by AUM desc
        sorted_funds = sorted(
            funds.items(),
            key=lambda x: -(x[1].get("total_value_usd", 0) if isinstance(x[1], dict) else 0)
        )
        for k, v in sorted_funds:
            if not isinstance(v, dict) or v.get("error"):
                continue
            aum = v.get("total_value_usd", 0)
            n = v.get("n_positions", 0)
            r.log(f"    {k:15s} {n:5d} pos  AUM ${aum/1e9:>9.1f}B")

        r.section("Top changes (most-bought + most-sold)")
        mb = (data.get("most_bought") or [])[:10]
        r.log(f"  Most bought (top 10):")
        for x in mb:
            n_buy = x.get("n_funds_adding", 0) + x.get("n_funds_new_position", 0)
            n_sell = x.get("n_funds_trimming", 0) + x.get("n_funds_exiting", 0)
            r.log(f"    {x.get('ticker','?'):8s} {x.get('name','')[:30]:30s} {x.get('n_funds_holding')} hold  +{n_buy} buying  -{n_sell} selling  ${x.get('total_value', 0)/1e9:.1f}B")
        ms = (data.get("most_sold") or [])[:10]
        r.log(f"\n  Most sold (top 10):")
        for x in ms:
            n_buy = x.get("n_funds_adding", 0) + x.get("n_funds_new_position", 0)
            n_sell = x.get("n_funds_trimming", 0) + x.get("n_funds_exiting", 0)
            r.log(f"    {x.get('ticker','?'):8s} {x.get('name','')[:30]:30s} {x.get('n_funds_holding')} hold  +{n_buy} buying  -{n_sell} selling  ${x.get('total_value', 0)/1e9:.1f}B")

        r.section("Sample BERKSHIRE top 5")
        brk = funds.get("BERKSHIRE", {})
        if isinstance(brk, dict) and brk.get("top_positions"):
            for p in brk["top_positions"][:5]:
                v = p.get("value_usd", 0)
                r.log(f"    {p.get('ticker','?'):8s} {p.get('name','')[:25]:25s} ${v/1e9:>8.2f}B  shares={p.get('shares', 0):>13,}")


if __name__ == "__main__":
    main()
