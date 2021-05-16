package tiger.sparkstreaming.proj.dao

import tiger.sparkstreaming.proj.entity.AreaCityAdCount

/**
 *
 * @Author Zenghu
 * @Date 2021/5/16 17:27
 * @Description
 * @Version: 1.0
 *
 * */
trait TAreaCityAdCountDao {

  def insert(areaCityAdCount: AreaCityAdCount): Boolean

  def getCount(areaCityAdCount: AreaCityAdCount): Int

}
