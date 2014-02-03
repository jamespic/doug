package uk.me.jamespic.dougng.viewmodel

/*
 * Subscription messages, for SubscribableVariables
 */
case class Subscribe(lastUpdate: Option[Long])
case object Unsubscribe
case class DataChanged(updateNo: Long, x: Any)