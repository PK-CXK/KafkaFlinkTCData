/**
 * Created by 乐 on 2019/12/5.
 */
public class UserBehavior {
    public Long userId;
    public Long itemId;
    public Long itemCate;
    public String type;

    public Long getUserId() {
        return userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public Long getItemCate() {
        return itemCate;
    }

    public String getType() {
        return type;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public Long timeStamp;
    public UserBehavior(){}
    public UserBehavior(Long l1,Long l2,Long l3,String s,Long l4){
        this.userId=l1;
        this.itemId=l2;
        this.itemCate=l3;
        this.type=s;
        this.timeStamp=l4;
    }
}
