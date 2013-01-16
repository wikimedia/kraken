package org.wikimedia.analytics.kraken.funnel;

import java.util.List;

public class Result {
	
	public final String userToken;
	public final List<FunnelPath> completionPaths;
    public boolean getHasFinishedFunnel(){
        return this.completionPaths != null && this.completionPaths.size() > 0;
    }

	public Result(String userToken, List<FunnelPath> completionPaths) {
		this.userToken = userToken;
		this.completionPaths = completionPaths;
	}
}
