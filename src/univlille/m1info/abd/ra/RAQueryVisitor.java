package univlille.m1info.abd.ra;

public interface RAQueryVisitor {
	
	public RAQuery nextQuery();
	public void switchToReadMode();
	
	public void visit (SelectionQuery q);
	public void visit (ProjectionQuery q);
	public void visit (JoinQuery q);
	public void visit (RenameQuery q);
	public void visit (RelationNameQuery q);

}
