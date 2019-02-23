package com.homework3.ylc.hive2mysql;

import java.io.Serializable;

public class UserBehavior implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String Uid;
	public long publish;
	public long view;
	public long comment;
	public String Aid;
	public String BehaviorTime;
	
	public UserBehavior()
	{
		this.Uid = "null";
		this.publish = 0;
		this.view = 0;
		this.comment = 0;
		this.Aid = "null";
		this.BehaviorTime = "null";
	}
	
	public static UserBehavior create(String uid, String publish,
			String view, String comment, String aid, String behaviortime)
	{
		UserBehavior ub = new UserBehavior();
		ub.Uid = uid;
		if(publish != null && publish.equals("null") == false)
			ub.publish = Integer.valueOf(publish);
		if(view != null && view.equals("null") == false)
			ub.view = Integer.valueOf(view);
		if(comment != null && comment.equals("null") == false)
			ub.comment = Integer.valueOf(comment);
		
		ub.Aid = aid;
		ub.BehaviorTime = behaviortime;
		return ub;
	}
	
	public void increasePublish()
	{
		this.publish += 1;
	}
	
	public void increaseView()
	{
		this.view += 1;
	}
	
	public void increaseComment()
	{
		this.comment += 1;
	}
	
	public String toString()
	{
		return ("Uid:" + this.Uid + 
				"|publish:" + String.valueOf(this.publish) + 
				"|view:" + String.valueOf(this.view) + 
				"|comment:" + String.valueOf(comment) +
				"|Aid:" + String.valueOf(this.Aid) + 
				"|time:" + this.BehaviorTime);
	}
	
	public void addByUid(UserBehavior x)
	{
		if(this.Uid.equals(x.Uid) == false)
			return;
		this.publish += x.publish;
		this.view += x.view;
		this.comment += x.comment;
	}
	
	public void addByAid(UserBehavior x)
	{
		if(this.Aid.equals(x.Aid) == false)
			return;
		this.publish += x.publish;
		this.publish = this.publish > 1 ? 1 : this.publish;
		this.view += x.view;
		this.comment += x.comment;
	}
}

