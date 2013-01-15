package org.wikimedia.analytics.kraken.funnel;

public class Result {
	
	public String userToken;
	public boolean hasFinishedFunnel;
	public int funnelPathId;
	
	public Result(String userToken, int funnelPathId, boolean hasFinishedFunnel) {
		this.userToken = userToken;
		this.funnelPathId = funnelPathId;
		this.hasFinishedFunnel = hasFinishedFunnel;
	}
}
